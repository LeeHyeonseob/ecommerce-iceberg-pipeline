# 운영 Runbook

## 실행

환경변수는 `.env.example`, 상세 명령은 루트 `README.md`를 따른다.

```text
Flink raw zone jobs 시작
→ Producer 실행
→ Airflow ecommerce_incremental 실행
→ health_check 로그와 Superset 운영 탭 확인
```

## 진단

- `verify_raw_zones.py`: Bronze 파일 크기와 도착 상태
- `health_check.py`: Silver/Gold freshness·파일 상태와 Iceberg metadata. `--detail`은 `detail/` 디렉터리의 파티션별 쿼리까지 실행하며, `--detail-tables`로 대상 테이블을, `--detail-partitions`로 테이블별 최근 파티션 수(기본 14)를 제한한다
- Airflow JSON 파싱 실패: Spark 로그의 선행 예외와 마지막 출력 확인
- 빈 배치: `batch_output_path=null`, Funnel·Gold skip 확인
- S3 오류: bucket, 자격증명, region, S3/S3A 설정 확인
- Superset 수치 중복: `ALL`, category, `dim_type` 필터 확인

## Iceberg 유지보수

재작성과 삭제는 성격이 달라 DAG를 나눴다. 재작성은 기존 파일을 즉시 물리 삭제하지 않고 새 snapshot을
생성하므로, snapshot 보존 기간 안에는 이전 상태를 조회할 수 있다. 삭제는 파일을 실제로 지우며,
cleanup이 지난 뒤에는 그 이전 상태로 돌아갈 수 없다. 재작성만으로는 저장 공간이 줄지 않고 오히려
늘어난다. 공간이 회수되는 것은 `expire_snapshots`가 스냅샷을 만료시킬 때다.

| DAG | 주기 | 단계 | 대상 |
| --- | --- | --- | --- |
| `iceberg_compaction` | 주 1회 (증분 `health_check` 성공 후 trigger) | `rewrite_data_files` → `rewrite_position_delete_files` | Silver 2개 |
| `iceberg_cleanup` | 주 1회 독립 cron (일 06:00 UTC) | `expire_snapshots` → `remove_orphan_files` | 7개 전부 |

- 순서는 입력 순서가 아니라 `iceberg_maintenance.py`의 `STEP_ORDER`로 고정된다. data file 재작성이
  dangling delete를 만들고 position delete 재작성이 그것을 정리하므로 뒤바꾸면 안 된다.
- Gold는 COW overwrite로 파티션당 data file이 1개라 `rewrite_data_files`가 구조적으로 0건이다.
  그래서 재작성 대상에서 제외한다.
- `rewrite_manifests`는 정기 실행하지 않는다. `commit.manifest-merge.enabled`(기본 true)와
  `commit.manifest.min-count-to-merge`(기본 100)로 쓰기 시 자동 병합되므로 무한 증가 위험이 없다.
  `iceberg/02_manifest_health.sql`로 개수를 관측하고, 필요하면
  `--steps rewrite_manifests`로 실행한다.
- 삭제는 `--as-of`로 기준 시각을 스케줄 시각에 고정한다. 넘기지 않으면 재시도마다 `now-30d`가
  다시 계산돼 범위가 넓어지고 로그만으로 범위를 재현할 수 없다.
- 증분과 재작성의 순서는 cron 시간차로 보장하지 않는다. Airflow 풀은 태스크 단위로 적용되므로
  1슬롯이 DAG 전체를 연속 구간으로 예약하지 않는다. 그래서 `TriggerDagRunOperator`로 연결한다.
- 주간 조건은 `dag_run.run_after`의 요일로 판단한다(`COMPACTION_WEEKDAY`, 기본 토요일).
  `data_interval_end`는 수동 트리거에서 정의되지 않아 쓰지 않는다. 스케줄 실행에서
  `run_after`는 트리거 시각이고 `logical_date`는 그보다 하루 앞이므로, 토요일 00:00 UTC에
  뜨는 실행이 컴팩션을 트리거한다.
- 주기는 확정값이 아니라 관측 후 조정할 초기값이다. 재작성 실행 시간과 실제 재작성량,
  파티션별 파일 수, Athena 조회 시간을 보고 정한다.

### 진단과 대응

- 그 주 증분이 실패하면 재작성도 트리거되지 않는다. 캐치업 경로가 없으므로 `iceberg_compaction`을
  수동 트리거한다.
- 테이블 하나가 실패해도 나머지는 계속 처리하고, 결과 JSON의 `failed_tables`에 기록된 뒤
  종료 코드 1로 끝난다. 실패한 테이블만 확인해 재실행한다.
- 삭제 단계 실패 시에는 재시도 전에 스냅샷·metadata 상태를 먼저 확인한다.

### 첫 실측 (기본 옵션) — 2026-09-13 완료

`iceberg_compaction`을 `silver_funnel`만 대상으로 옵션 없이 수동 트리거했다.
DAG가 `health_before → rewrite → health_after` 순서로 돌므로 전후 상태가 같은 DAG run의
`health_before`와 `health_after` task 로그에 남는다.

procedure 반환값:

| 단계 | 결과 | 소요 |
| --- | --- | ---: |
| `rewrite_data_files` | 78개 재작성 → 26개 생성, 796,814,217 bytes, 실패 0 | 55.7초 |
| `rewrite_position_delete_files` | 0건 | 0.4초 |

파일 상태 전후:

| 지표 | 전 | 후 |
| --- | ---: | ---: |
| data file 수 | 83 | 31 |
| 복수 파일 파티션 | 26 | 0 |
| 파티션 최대 파일 수 | 3 | 1 |
| 평균 data file 크기 | 10.75MB | 28.73MB |
| data file record 합계 | 27,815,672 | 27,785,942 |

**기본 옵션으로 재작성이 일어났다.** 사전 예측은 `min-input-files=5` 때문에 0건이었으나
실제로는 파티션당 파일 3개가 전부 대상이 됐다. 파일이 목표 크기(128MB)에 크게 못 미치는
것이 선정 조건으로 작동한 것으로 보인다. 따라서 잔재 정리를 위한 data file 옵션 조정은
필요하지 않았다.

record 합계가 29,730건 줄었는데 이는 position delete 레코드 수와 정확히 같고, 결과값
27,785,942는 MOR 전환 전 원래 행 수와 일치한다. 재작성이 delete를 실제로 적용했다는 증거다.
파티션을 넘어 병합하지 않으므로 평균 크기는 목표 128MB가 아니라 파티션당 데이터량인
28.73MB가 됐다. 이것이 정상이다.

**남은 문제였던 것**: 첫 실행 후 스냅샷의 `total-delete-files`가 여전히 52, `total-position-deletes`가
29,730이었다. data file 재작성이 delete를 적용하면서 옛 delete file이 dangling으로 남았는데
`rewrite_position_delete_files`가 기본 옵션에서 0건을 반환했기 때문이다. 아래 2차 실행으로 해소했다.

### 2차 실행 (delete 옵션 조정) — 2026-09-13 완료

`delete_rewrite_options=min-input-files=2`만 적용하고 `rewrite_options`는 비워 재실행했다.

| 단계 | 결과 | 소요 |
| --- | --- | ---: |
| `rewrite_data_files` | 0건 | 2.2초 |
| `rewrite_position_delete_files` | 52개 재작성 → 0개 생성, 148,832 bytes | 7.1초 |

`added_delete_files_count=0`이다. 52개 전부 dangling이어서 병합 대상이 아니라 제거 대상이었다.
재작성 바이트 148,832는 docs/failures/003에 기록된 delete bytes와 일치한다.
data file은 이미 파티션당 1개라 0건으로 끝났다 — 무작업일 때 2.2초라는 고정 비용도 여기서 확인된다.

최종 상태:

| 지표 | MOR 전환 전 | 재처리 테스트 후 | 1차 후 | 2차 후 |
| --- | ---: | ---: | ---: | ---: |
| data file | 31 | 83 | 31 | 31 |
| delete file | 0 | 52 | 52 | **0** |
| position delete record | 0 | 29,730 | 29,730 | **0** |
| record 합계 | 27,785,942 | 27,815,672 | 27,785,942 | 27,785,942 |

재처리 테스트 잔재가 모두 정리돼 MOR 전환 직전 상태로 돌아왔다. 이제부터의 일별 누적이
기본 옵션 관측의 깨끗한 기준선이다.

### 증분 스케줄 가동과 trigger 경로 검증 — 2026-09-13 완료

`ecommerce_incremental`을 unpause하자 `run_after=2026-09-13 00:00`(logical_date 2026-09-12)
실행이 즉시 떨어져 35초에 성공했다. `CronTriggerTimetable`은 가장 최근 실행 가능 시점을
바로 스케줄한다.

| 태스크 | 상태 | 소요 |
| --- | --- | ---: |
| `silver_events` | success | 12.5초 |
| `silver_funnel` | success (skip 분기) | 0.1초 |
| `gold` | success (skip 분기) | 0.1초 |
| `health_check` | success | 17.9초 |
| `is_compaction_day` | success | 0.1초 |
| `trigger_compaction` | **skipped** | — |

Bronze에 해당 수집 시각 구간의 데이터가 없어 `{"batch_output_path": null, "event_count": 0,
"event_dates": []}`로 끝났고, Funnel·Gold가 설계대로 건너뛰었다. `trigger_compaction`이
skipped인 것도 정확하다 — `run_after`가 일요일이라 토요일 조건에 걸리지 않는다.

컴팩션이 실제로 트리거되는지는 토요일 실행분에서 확인해야 한다. 일별 파일 누적 관측도
Bronze에 새 데이터가 들어와야 시작된다.

### cleanup 경로 검증 — 2026-09-13 완료

만료 대상이 없는 상태에서 `iceberg_cleanup`을 수동 트리거해 실행 경로만 확인했다.
가장 오래된 스냅샷이 2026-08-25라 30일 기준에 미달이고, `older_than`(2026-08-14)이
테이블 생성 이전이라 orphan도 없다.

| 항목 | 결과 |
| --- | --- |
| 대상 | 7개 테이블 전부, 실패 0 |
| `expire_snapshots` | 전 테이블 삭제 0건, 테이블당 1.0~6.0초 |
| `remove_orphan_files` | 전 테이블 삭제 0건, 테이블당 1.0~2.2초 |
| 합계 | 22.9초 |
| `--as-of` | `run_after`에서 2026-09-13 14:44:52, `older_than` 2026-08-14 14:44:52로 정확히 계산됨 |

`remove_orphan_files`는 테이블 S3 경로를 리스팅하므로 할 일이 없어도 비용이 든다고 봤으나,
현재 규모에서는 테이블당 1~2초다.

첫 실행에서 `data_interval_end`가 수동 트리거에 정의되지 않아 Jinja 렌더링 단계에서 실패했다.
`dag_run.run_after`로 바꿔 해결했다. `CronTriggerTimetable`에서는 두 값이 같고 `run_after`는
수동 트리거에서도 정의된다. 같은 문제가 있던 `ecommerce_incremental`의 `is_compaction_day`도
함께 고쳤다.

### 이후 옵션 정책

data file은 **기본 옵션을 유지한다.** 실측에서 목표 크기 미달이 선정 조건으로 작동해
파티션당 3개가 전부 재작성됐다. `min-input-files` 조정이 필요하지 않다.

position delete는 기본 옵션(`min-input-files=5`)으로는 dangling delete가 정리되지 않는다.
정기 실행에서 delete file이 누적되는 것이 관측되면 `delete_rewrite_options=min-input-files=2`를
검토한다. 상시 적용 여부는 일별 누적 속도를 본 뒤 정한다.

옵션은 한 번에 하나씩만 바꾼다. `delete-file-threshold=1`의 상시 적용은 MOR의 쓰기 절감
효과를 잃으므로 쓰지 않는다. `docker exec` 직접 실행은 `spark_pool`을 우회해 증분과 겹칠 수
있으므로 쓰지 않는다.

## Grafana·Prometheus 모니터링

Kafka·Flink 실시간 지표는 `infra/docker-compose.monitoring.yml`(Prometheus, kafka-exporter,
Grafana)과 `infra/kafka.Dockerfile`(JMX exporter를 javaagent로 얹은 Kafka 이미지)로 구성한다.
대시보드 정의는 `monitoring/grafana/dashboards/streaming-operations.json`에 코드로 보관한다.

### JMX exporter 구성 확인 — 2026-09-14 완료

`kafka-broker` scrape 대상(`kafka:9404`)이 `up` 상태로 확인됐다. `infra/kafka-jmx.yml`은
request handler 여유율, ISR 증감, under-replicated 파티션, produce/fetch 요청 지연,
메시지·바이트 처리량을 노출한다. `kafka-jmx.yml`의 커스텀 규칙에는 없지만, JMX Exporter의
기본 JVM 지표로 heap·GC도 함께 수집된다(아래 상세 진단 대시보드 참고).

**단일 브로커·복제계수 1 환경이라 ISR·복제 관련 지표는 구조적으로 항상 0이다.** 브로커가
하나뿐이라 복제본이 줄어들거나(shrink) 늘어날(expand) 대상 자체가 없다. 계측 자체는 무해하고
브로커를 늘릴 때를 대비한 것이지만, 지금은 신호로 쓸 수 없다. 대시보드의 "Kafka 복제 이상
파티션" 패널 설명에도 명시했다.

### 정상 속도 부하 베이스라인 — 2026-09-14 완료

정확한 임계값을 정하기엔 실측이 한 번뿐이라 부족하다고 판단해, **느슨한 sanity 임계값**(명백히
고장난 상태만 표시)만 잡고 정밀한 warning/critical은 실제 운영 이력이 쌓인 뒤로 미뤘다.

측정 방법: `2019-Dec.csv.gz`의 시간대별 분포를 스캔해 실제 피크 시간대(14~17시, 자정 대비
약 12배)를 확인한 뒤, 12월 1일 15:00~15:31 구간만 잘라 `--speed 1`(원본 타임스탬프 간격 그대로)
로 재생했다. 배속을 왜곡하면 lag·backpressure 같은 도착률 의존 지표가 실제와 달라지므로,
자정처럼 트래픽이 적은 시간대에서 실시간 대기하는 대신 피크 시간대만 골라 실시간으로 재생했다.
`kafka_producer.py`는 건드리지 않고 임시 슬라이스 파일만 만들어 썼다.

| 지표 | 관측값 |
| --- | --- |
| 배속 정확도 | event 경과 31분19초 / wall 경과 31분42초 = 0.99배 |
| 처리량 (view, 트래픽 대부분) | 평균 27.15/s, 최대 32/s |
| Kafka consumer lag | 순간 최대 677, 종료 후 0 |
| request handler 여유율 | 평균 99.99%, 최저 99.99% |
| backpressure | 전 구간 0 |
| checkpoint 실패 | 0건 |
| checkpoint 소요 (정상) | 179~500ms (68회 중 67회) |
| checkpoint 소요 (이상치) | 5,340ms 1회, 전후 정상 — 지속 아님 |

이 데이터셋의 "피크"(초당 33건)는 로컬 단일 브로커 Kafka에는 사실상 부하가 아니다. lag는
즉시 소화되고 idle%는 100%에 붙어 있다. 60배속 테스트에서 겪은 OOM은 Kafka/Flink가 아니라
Spark 메모리 쪽 문제였다(위 Iceberg 재작성 실측 참고).

체크포인트 이상치는 68회 중 1회(약 1.4%)가 정상 부하에서도 발생할 수 있다는 실증 근거다.
순간값 기반 알림이면 이 정도 빈도로 오탐이 난다 — 알림 규칙에서 지속 조건(연속 2회 이상)을
쓸 근거가 된다.

**표본 1회로는 정밀한 임계값을 정할 수 없다.** 하루·요일별 변동, 반복 실행 시 재현성, 로컬
Docker Desktop 환경의 호스트 노이즈, 실제 이상 상황(S3 지연·네트워크 문제)에서의 동작을 전혀
확인하지 못했다. 아래 임계값은 전부 "명백히 고장난 상태만 표시"하는 sanity 수준이며 확정이
아니다.

### 반영한 sanity 임계값

`monitoring/grafana/dashboards/streaming-operations.json` 수정.

| 패널 | 이전 | 변경 후 | 근거 |
| --- | --- | --- | --- |
| Kafka consumer lag | yellow 1,000 / red 10,000 | yellow 10,000 / red 50,000 | 실측 정상 피크 순간값(677)의 15~70배로 벌려 정상 튐과 구분. `lastNotNull` 순간값 패널이라 지속 조건 없이 타이트하게 잡으면 오탐 |
| checkpoint 시간 | 없음 | yellow 5,000ms / red 15,000ms (신규) | 체크포인트 주기 30초의 절반을 넘는 수준만 표시. 정상 대역(179~500ms)과 관측된 단발 이상치(5,340ms) 사이에 여유를 둠 |
| Kafka 복제 이상 파티션 | green/red 그대로 | 값 변경 없음, 설명만 추가 | 위 참고 — 지금은 항상 0 |

**그대로 둔 것**: request handler 여유율(green≥50%/yellow 30~50%/red<30% — 일반적인 Kafka
운영 관례값과 일치, 실측 99.99%에서 한참 여유), 체크포인트 실패(`increase(...[5m])`로 이미
5분 누적이라 순간값 문제 없음), backpressure(타임시리즈 라인 색칠이라 상대적으로 안전).

### 상세 진단 대시보드 — 2026-09-14 완료

메인 화면(`streaming-operations.json`)과 분리한 `monitoring/grafana/dashboards/kafka-diagnostics.json`
(`Kafka/Flink 상세 진단`)을 추가했다. 토픽별 메시지 처리량, 브로커 바이트 in/out, Produce/Fetch
요청 지연, ISR 증감, JVM heap 사용량, GC 시간 비율 6개 패널.

**JVM heap·GC는 추가 계측 없이 이미 수집되고 있었다.** jmx_exporter javaagent가 커스텀
`kafka-jmx.yml` 설정과 무관하게 기본으로 `jvm_memory_used_bytes`, `jvm_gc_collection_seconds_*`를
내보내고(`DefaultExports`), Flink Prometheus reporter도 `flink_*_Status_JVM_Memory_Heap_*`,
`flink_*_Status_JVM_GarbageCollector_*`를 기본 제공한다. 이미지 재빌드나 설정 변경 없이
대시보드 파일 추가만으로 끝났다.

Produce/Fetch 요청 지연 패널에는 `FetchConsumer`의 p50이 약 500ms 근처로 나오는 게 정상이라는
설명을 달았다 — `fetch.max.wait.ms`(기본 500ms) long-poll 설계 때문이지 실제 지연이 아니다.
ISR 증감 패널에는 단일 브로커·복제계수 1이라 구조적으로 항상 0이라는 설명을 재확인해 뒀다.

전체 6개 패널 쿼리를 Prometheus에 직접 질의해 실제 시리즈가 반환되는지 확인했다(1~12개 시리즈,
빈 응답 없음).

### 알림 규칙 — 2026-09-14 완료

`monitoring/grafana/provisioning/alerting/sanity-rules.yml`에 Grafana Alerting 규칙 5개를
코드로 프로비저닝했다. 값은 위 sanity 임계값과 맞췄다.

| 규칙 | 조건 | 지속(`for`) |
| --- | --- | ---: |
| Kafka consumer lag 지속 | `sum(kafka_consumergroup_lag_sum) > 10000` | 5분 |
| RUNNING Flink 잡 부족 | `count(flink_jobmanager_job_uptime) < 3` | 2분 |
| Flink checkpoint 실패 | `increase(...numberOfFailedCheckpoints[5m]) > 0` | 1분 |
| Flink backpressure 지속 | `max(...backPressuredTimeMsPerSecond) > 500` | 5분 |
| Kafka 복제 이상 파티션 | `sum(...underreplicatedpartitions) > 0` | 1분 |

마지막 규칙은 단일 브로커·복제계수 1에서는 구조적으로 발동 불가하다는 주석을 규칙 파일에
남겼다 — 브로커를 늘렸을 때를 대비한 정의다.

**알림 규칙은 대시보드와 달리 파일 변경이 핫리로드되지 않는다.** `docker restart grafana`가
있어야 새 프로비저닝이 반영된다(컨테이너 재생성까지는 필요 없다).

**실제 장애를 일으켜 생명주기 전체를 검증했다.** `raw_zone_cart` Flink 잡을 강제로 취소해
"RUNNING Flink 잡 부족" 규칙으로 확인했다.

```
잡 취소 → count(flink_jobmanager_job_uptime) = 2로 하락
inactive → pending (2분 대기 시작)
2분 경과 → pending → firing   (for: 2m대로 정확히 동작)
잡 재제출 → 메트릭 3으로 즉시 복구
firing → inactive             (추가 지연 없이 즉시 해제)
```

컨슈머 그룹은 `group-offsets` 방식이라 재제출 후 끊김 없이 이어졌고 lag는 0으로 확인됐다.

### Slack 연동 — 2026-09-14 완료

`monitoring/grafana/provisioning/alerting/`에 `contact-points.yml`(Slack Contact Point),
`notification-policies.yml`(기본 정책이 전 알림을 `slack-operations`로 라우팅),
`templates.yml`(`ecommerce.slack.title`/`.text`) 3개 파일로 프로비저닝했다.

secret은 저장소에 두지 않는다. `.env`의 `SLACK_WEBHOOK_URL`을
`infra/docker-compose.monitoring.yml`이 `SLACK_WEBHOOK_URL: ${SLACK_WEBHOOK_URL:?SLACK_WEBHOOK_URL
must be set}`로 Grafana 컨테이너에 주입하고, `contact-points.yml`은 `$SLACK_WEBHOOK_URL` 치환
문법으로 참조한다. Grafana Contact Point API로 조회하면 `"url": "[REDACTED]"`로 나와 실제
치환·저장이 확인된다 — 값 자체는 API 응답에도 노출되지 않는다.

**실제 Slack 채널에서 전달을 확인했다(2026-09-14 23:22~23:27 KST).** 위 "RUNNING Flink 잡
부족" 재현 과정에서 FIRING 메시지가 잡 취소 후 5분 뒤 도착했고(그룹핑 `group_wait: 30s` 포함),
잡 복구 후 RESOLVED 메시지가 도착했다. 규칙명·심각도(critical)·`summary` 문구·Grafana 버전이
템플릿대로 렌더링됐다.

### 남은 항목

- JVM heap·GC 관측은 됐으나 해당 패널에 sanity 임계값(색상 표시)은 아직 없음
- Airflow DAG 실패·재시도 초과 알림, Bronze freshness 임계값 알림은 아직 없음 (Grafana/Kafka/Flink
  경로만 완료)
- 정밀 임계값은 실제 운영 이력이 쌓인 뒤 재검토

## Airflow TaskInstance 직접 조작으로 인한 오류 종료 — 2026-09-15

11월 재생 데이터 정합성 복구 도중, 재처리 중이던 `silver_funnel` 태스크가 90% 가량
진행된 상태에서 SIGTERM으로 갑자기 종료됐다.

**원인**: DAG를 unpause한 직후 스케줄러가 이미 `up_for_retry` 상태를 자동으로 재개해
정상 진행 중이었는데, 별도로 `ti.state = None`처럼 TaskInstance를 raw SQLAlchemy ORM으로
직접 덮어썼다. Airflow 3의 워커는 API 서버에 "이 시도가 여전히 유효한지" 계속 확인하는
구조라, ORM으로 어긋난 상태를 스케줄러가 "not_running"으로 오인해 진행 중인 프로세스를
강제 종료시켰다.

**대응**: TaskInstance는 raw ORM으로 직접 고치지 않는다. 정말 초기화가 필요하면
Airflow가 CLI/UI 내부에서 쓰는 정식 함수(`airflow.models.taskinstance.clear_task_instances`)를
쓰거나, `logical_date`가 있는 run이면 `airflow tasks clear` CLI를 쓴다. 대부분은 개입 없이
두면 스케줄러가 `up_for_retry`를 알아서 재시도한다.

이후 같은 정합성 복구 과정에서 실제로 완전히 종결(`failed`)된 태스크를 재시도시킬 때는
`clear_task_instances`를 직접 호출해 안전하게 재개했다(RUNNING 상태 태스크에 쓰면
`RESTARTING`으로 안전하게 전환하는 보호 로직이 내장돼 있다).

## Slack 알림 문구 개선 — 2026-09-16

기존 템플릿(`templates.yml`)은 규칙명·심각도·summary를 그대로 나열해 딱딱했다. 이
규모에서는 조치 방법·Runbook·Silence 링크까지 넣는 건 과하다고 판단해, 제목은
`[심각도][상태] 알림명`, 본문은 FIRING일 때만 summary와 현재값(`.Values.A`, 규칙의
원본 메트릭 refId)을 보여주고 RESOLVED일 때는 "정상 복구되었습니다."만 보여주는
최소 구성으로 정리했다.

**주의**: `notification-policies.yml`의 `group_interval: 5m` 때문에, 최초 FIRING 발송
이후 같은 그룹(alertname+severity)의 다음 업데이트(RESOLVED 포함)는 최소 5분 뒤에나
나간다. 복구 알림이 안 왔다고 판단하기 전에 5분 이상 기다려야 한다 — 실제로 이 시간을
못 채우고 스택을 내렸다가 복구 메시지를 놓친 적이 있다.

실제 Flink 잡 취소·복구로 FIRING·RESOLVED 두 케이스 모두 새 문구가 Slack에 그대로
도달하는 것을 확인했다.

## Flink 레코드 단위 DLQ 도입 — 2026-09-16~17

`code/pipelines/raw_zone_consumer.py`가 `format=json` + `json.ignore-parse-errors=true`로
Kafka를 읽어, JSON 파싱에 실패한 레코드가 SQL에 도달하기도 전에 커넥터(정확히는 JSON
역직렬화 계층) 단에서 조용히 사라지는 문제가 있었다. `format=raw`로 원문을 문자열
그대로 받고 직접 파싱·검증한 뒤 `StatementSet`으로 정상은 Bronze, 실패는
`ecommerce.events.dlq`로 분기하도록 바꿨다.

### StatementSet이 Kafka source를 공유하는지 사전 검증 (가장 위험한 전제)

격리된 테스트 토픽(운영과 동일한 3파티션)에서 정상 10건 + 실패 4건을 섞어 검증했다.

- `stmt_set.explain()`의 **"Optimized Execution Plan"** 섹션에서 `TableSourceScan`이
  정확히 1개, 두 INSERT 분기 모두 `Reused(reference_id=[1])`로 참조 — AST/물리 plan
  섹션까지 합쳐서 세면 여러 번 나오므로 반드시 마지막 섹션만 봐야 한다
- 실제로 흘려서 `source offset 집합 = ok offset 집합 ∪ dlq offset 집합`,
  `ok ∩ dlq = 공집합` 확인 (건수 비교가 아니라 집합 비교 — at-least-once + 체크포인트
  재생으로 같은 sink 안에서 offset이 중복될 수 있어 건수만 보면 오판할 수 있음)
- 실행 중 Flink task 이름을 보면 source부터 Bronze/DLQ writer까지 물리적으로 하나의
  task chain으로 융합돼 있어 런타임에서도 공유가 재확인됨

Flink 1.19.3 + flink-sql-connector-kafka 3.2.0-1.19 조합에서 위 전제가 실측으로
확정돼, DataStream+side output이나 별도 group.id 같은 우회 없이 Table API 구조를
그대로 유지했다. `table.optimizer.reuse-sub-plan-enabled`/`reuse-source-enabled`는
현재 기본값도 true지만, 향후 Flink 업그레이드가 기본값을 바꿔도 조용히 깨지지 않도록
코드에 명시했다.

### 실제로 재현해서 잡은 회귀

- **Row 인코딩**: UDF가 ROW 타입을 반환할 때 일반 Python 튜플을 쓰면
  `AttributeError: 'tuple' object has no attribute 'get_fields_by_names'`로 job이
  죽는다. `pyflink.table.Row`를 써야 한다.
- **타입 불일치로 인한 job 크래시**: `product_id`처럼 STRING 계약인 필드가 JSON에서
  숫자(`"product_id": 1001`)로 오면 `AttributeError: 'int' object has no attribute
  'encode'`로 Beam Row 코더 단계에서 job이 죽는다 — UDF의 `try/except`로는 못 잡는다
  (코더 직렬화가 UDF `eval()` 밖에서 일어남). STRING Row 슬롯에 들어갈 값은 UDF
  안에서 미리 명시적으로 문자열 강제 변환해야 한다(dict/list처럼 강제 변환이 의미를
  바꾸는 타입은 누락으로 취급). 두 경우 모두 격리된 테스트 토픽으로 직접 재현해서
  고친 뒤 재검증했다.

### DLQ 토픽은 저장소에 프로비저닝 코드가 없다 — 수동 생성 필수

`ecommerce.events.dlq`는 이 환경에서 수동으로 만든 것이라 새 환경(다른 macOS, CI 등)에는
없다. auto-create에 맡기면 브로커 기본 `log.retention.hours=168`(7일)로 생겨 DLQ의
"유일한 사본" 요구를 어긴다. **job 배포 전에 반드시 먼저** 아래로 만든다.

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic ecommerce.events.dlq --partitions 3 --replication-factor 1 \
  --config retention.ms=2592000000 --config cleanup.policy=delete

# 검증 - Dynamic configs에 retention.ms=2592000000이 오버라이드로 찍혀야 한다
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic ecommerce.events.dlq
```

원본 3개 토픽(`ecommerce.view/cart/purchase`)도 여전히 프로비저닝 코드가 없고
auto-create에 의존한다 — 이번 범위 밖이라 손대지 않았다. 실측 결과 이 토픽들은
브로커 기본 `num.partitions=1`이 아니라 3파티션인데, 그걸 만든 코드도 저장소에 없다
(과거 수동 작업으로 추정). 다음에 토픽 프로비저닝을 정식화할 때 DLQ와 같이 묶을 후보다.

### 통합 DLQ 토픽 1개로 결정 (토픽별 3개 대신)

`original_topic` 컬럼으로 출처를 구분할 수 있고, 단일 브로커 환경이라 토픽을 나눠도
격리 이점이 없다(이미 브로커를 공유함). retention·운영 정책도 동일해서 나눌 이유가
없다.
