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

### partition pruning 실측 — 2026-09-17 완료

`silver_events_to_funnel.py`의 `build_propagated_keys`/`read_purchase_evidence`가 쓰는
`event_date`/`funnel_date` `.isin(partitions)` 필터가 실제로 Iceberg partition pruning을
타는지 의심 지점이었다(안 타면 27M+ 행 전체 스캔이 될 수 있어 디스크 고갈의 유력 원인).
`spark-runner` 컨테이너에서 실제 `glue.ecommerce_lakehouse.silver_events`/`silver_funnel`에
직접 필터를 걸어 확인했다.

`df.inputFiles()`는 Iceberg V2 소스에서 항상 빈 배열을 반환해 지표로 쓸 수 없었다. 대신
`df.rdd.getNumPartitions()`(플랜만 실행, 잡 실행 없이 스캔 입력 분할 수를 알려줌)로 재측정:

| 케이스 | 입력 파티션(task) 수 |
| --- | ---: |
| `silver_events` 전체 (33일, 파일 47개) | 37 |
| `event_date IN (3일치)` | 4 |
| `event_date = 1일치` | 2 |
| `silver_funnel` 전체 | 52 |
| `funnel_date IN (2일치)` | 4 |

물리 실행 계획에도 `BatchScan ... [filters=event_date IN (18170, 18171, 18172)]`로 필터가
push-down된 것이 보이고, `<table>.files` 메타데이터 쿼리로 대조한 해당 날짜의 실제 파일 수(3+1+1=5)와
스캔 입력 개수가 일치한다. **partition pruning은 정상 작동한다.** 디스크 고갈 원인에서 제외.

남은 용의자는 두 함수의 JOIN·DISTINCT가 만드는 shuffle spill이다(원래 추정과 동일). 다음 조사는
이 경로의 실제 shuffle write 바이트량을 실측하는 것부터 시작한다 — `docs/TODO.md`의
"작은 시간 구간 재처리의 불필요한 I/O 축소" 항목 참고.

### 재처리 shuffle 재현 — 2026-09-17 완료

과거 "88만 건 재처리" 때 관측된 디스크 급증이 실제로 재현되는지, `test-incremental` 환경에서
같은 규모로 재현했다. `silver_funnel_test_incremental`(13,424,825건, `purchased=0` 13,094,019건)에서
`purchased=0`인 `(user_id, product_id)` 88만 쌍을 뽑아 새 `user_session`(다른 세션)을 붙이고
`event_date=2019-10-15`(테스트 데이터의 마지막 날)로 합성한 purchase 배치를 만들어
`silver_events_to_funnel.py --mode incremental --env test-incremental`로 실행했다.

`spark-runner` 컨테이너의 `df -h /`를 20초 간격으로 관측:

| 경과 | 디스크 사용량 |
| --- | ---: |
| 시작 전 | 25G / 48G |
| +100초 | 30G |
| +140초 (peak) | 33G |
| 완료 직후 | 25G (완전 회수) |

약 8GB가 일시적으로 증가했다가 job 종료와 동시에 즉시 회수됐다 — 디스크 누수가 아니라
shuffle 임시 파일(`/tmp/blockmgr-*`, root overlay와 같은 볼륨)이 원인임을 확인. 실행 로그에는
기본값 `spark.sql.shuffle.partitions=200`짜리 스테이지가 여러 번(Stage 24/45/61/79/80/108/147/165/191)
등장했다 — JOIN·DISTINCT의 shuffle stage가 기본 200개 reduce partition으로 계획된 것을
관측했을 뿐, partition 수를 바꿔가며 비교하는 격리 실험은 하지 않았다. `spark.sql.shuffle.partitions`는
reduce 쪽 task/partition 개수를 정하는 것이지 물리 shuffle 파일 수가 그대로 200개라는
뜻은 아니다 — 이 관측은 "reduce 단계가 잘게 쪼개진다"는 증거이지, 200이라는 값 자체가
8GB 증가분의 원인이라고 확정할 근거는 아니다.

`영향받은 키=1,953,080 재계산된 funnel=1,073,080`이었고, **영향받은 `funnel_date`가 테스트
테이블에 있는 15일 전부**였다. 구매일(10-15) 기준 과거 30일 역방향 윈도우가 테스트 데이터
전체 기간을 덮어버렸기 때문 — partition pruning은 정상 작동했지만(위 절 참고), 애초에
`.isin(partitions)`로 넘기는 파티션 목록 자체가 테이블 전체를 커버해버리면 프루닝의 이점이 없다.
프로덕션(33일 분량)에서 월말 근처 구매가 몰리면 같은 패턴으로 더 넓은 범위가 걸릴 수 있다.

**결론**: 디스크 급증은 실제 데이터 누수가 아니라, 넓은 재처리 범위가 만드는 대용량
JOIN/DISTINCT의 shuffle이 원인이다. 기본 200-partition은 그 shuffle을 작은 파일 여러 개로
쪼개는 관측된 요소일 뿐, 200이라는 값 자체가 8GB 증가분의 근본 원인이라고 확정할 근거는
없다 — partition 수를 낮춰가며 비교하는 격리 실험은 하지 않았다. 정상적인 소규모 일일
배치라면 영향 범위가 좁아 문제가 안 되지만, 캐치업처럼 넓은 날짜 범위의 구매가 한 배치에
몰리면 재현된다. 개선 방향(착수 전, 실측만 완료): `read_purchase_evidence`의
`candidate_funnels`를 브로드캐스트 가능한 크기로 필터링해 shuffle join 대신 broadcast join
유도, 재처리 배치 크기 자체를 좁혀 캐치업을 여러 번에 나눠 도는 방안. `spark.sql.shuffle.partitions`
하향은 후보에서 제외한다 — 파티션 수를 줄이면 파티션당 데이터가 커져 오히려 개별 파티션의
spill/OOM 위험이 늘어날 수 있어, 위 두 원인 자체를 줄이는 방향이 아니라면 역효과가 날 수 있다.
스크립트는 재사용 목적의 영구 파일로 남기지 않았다(1회성 진단, `/tmp` 스크래치로 실행 후 S3
임시 배치 삭제).

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
- 정밀 임계값은 실제 운영 이력이 쌓인 뒤 재검토

### Bronze freshness 알림 추가 — 2026-09-17

Kafka lag·Flink 잡 수 알림은 소비 지연이나 잡 죽음은 잡지만, producer가 멈춰서 Kafka에
새 메시지 자체가 안 들어오는 경우(lag=0, 잡은 RUNNING)는 못 잡는 사각지대였다. 새 exporter
없이 DLQ 작업 때 이미 추가해 둔 `raw_zone_consumer.py`의 `business.event_count`(job당 1개)를
재사용했다 — Prometheus에는 `flink_taskmanager_job_task_operator_business_event_count{job_name=...}`로
노출된다.

`sanity-rules.yml`에 `sanity-bronze-freshness` 규칙을 추가했다:
`min(sum by (job_name) (increase(...[10m])))`가 1 미만인 상태가 5분 지속되면 발동 —
view/cart/purchase 세 job 중 하나라도 10분간 신규 이벤트가 0건이면 잡힌다.

**실제로 검증했다.** Grafana 재기동 시점에 producer가 꺼져 있어 자연스럽게 firing까지
재현됐다(pending 8회 관측 후 firing 전환, 실제 Slack에 `[WARNING][FIRING]` 도착 확인).
이후 producer를 짧게(5,000건, `--speed 3000`) 돌려 세 job 카운터를 모두 증가시켰고,
Grafana 룰 상태가 `inactive`로 돌아온 뒤 `group_interval: 5m`이 지나서 RESOLVED가
`[WARNING][RESOLVED]`로 도착하는 것까지 확인했다. RESOLVED에도 severity가 붙는 건
버그가 아니라 `ecommerce.slack.title` 템플릿이 상태와 무관하게 항상 `[심각도][상태]`를
보여주도록 설계된 것이다(기존 5개 알림도 전부 동일하게 동작).

이 알림도 severity는 `warning`이다 — producer를 의도적으로 멈춘 상태(데모 종료 등)에서도
울리기 때문에, critical로 두면 오탐이 잦다.

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

`code/pipelines/ingestion/raw_zone_consumer.py`가 `format=json` + `json.ignore-parse-errors=true`로
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

## Airflow 태스크 실패 Slack 알림 도입 — 2026-09-17

Grafana/Kafka/Flink 경로 알림은 있었지만 Airflow DAG 자체의 실행 실패(예: `docker exec
spark-runner`가 죽거나 Spark 잡이 예외로 끝나는 경우)는 알림 경로가 없었다. 새 Prometheus
exporter를 추가하는 대신, Airflow의 `on_failure_callback`으로 직접 Slack Incoming Webhook에
POST하는 방식을 택했다 — 이미 `.env`의 `SLACK_WEBHOOK_URL`이 있고, `requests`가 Airflow 코어의
전이 의존성으로 이미 설치돼 있어 새 인프라(exporter, statsd 등) 없이 끝난다.

`airflow/dags/dag_utils.py`에 `slack_alert_on_failure(context)`를 추가하고 세 DAG
(`ecommerce_incremental`, `iceberg_compaction`, `iceberg_cleanup`)의 `default_args`에
`on_failure_callback`으로 등록했다. `on_failure_callback`은 Airflow 소스(`task_runner.py`의
`finalize()`)상 `state == FAILED`일 때만 호출되고 `UP_FOR_RETRY`는 별도의 `on_retry_callback`
경로다 — 재시도마다 알림이 오지 않고 재시도를 모두 소진한 최종 실패에서만 온다는 게 코드로
확인된다. `requests`는 전이 의존성에만 있던 걸 `infra/airflow/requirements.txt`에 직접 pin으로
추가했다(버전이 바뀌어도 깨지지 않게).

**실제 실패로 검증했다.** 항상 실패하는 1태스크짜리 임시 DAG(`_test_slack_alert`, `retries=0`)를
띄워 컨테이너에 반영되는 것과 트리거 후 실패 상태(`airflow dags list-runs`로 `failed` 확인)까지
본 뒤, 실제 Slack 채널에 다음 형식으로 도달하는 것을 확인했다:

```
[CRITICAL][FIRING] Airflow 태스크 실패
{dag_id}.{task_id} (run_id=..., try=1)
{예외 메시지}
{ti.log_url}
```

검증 후 `_test_slack_alert` DAG은 `airflow dags delete`로 메타데이터에서 제거하고 파일도
삭제했다 — 저장소에는 커밋되지 않는다.

**1차 검증 직후 발견한 문제**: 위 payload를 `{"text": text}`로 단순 전송했더니 Slack에
메시지는 도달했지만 Grafana Slack 알림에 있는 빨간 색상바가 없었다. Grafana의 Slack
연동은 알림 상태에 따라 자동으로 attachment 색상바를 붙이는데, 직접 만든 webhook 호출은
그 처리가 없다. `payload = {"attachments": [{"color": "danger", "text": text}]}`로 바꿔
Slack의 사전 정의 색상명(`danger`=빨강)을 쓰는 attachment 형식으로 재전송하도록 고쳤고,
같은 임시 DAG으로 재검증해 실제로 빨간 색상바가 나오는 것을 확인했다(스크린샷 확인, 저장소엔
보관 안 함).

**주의(내 실수, README는 정확함)**: `docker compose -f infra/docker-compose.airflow.yml up -d`를
`set -a; source .env; set +a` 없이 바로 실행하면 컴포즈 파일 안의 `${AIRFLOW_POSTGRES_PASSWORD}`
등이 빈 값으로 치환돼 `airflow-init`이 DB 연결 실패로 죽는다. README의 실행 순서(`source .env`
먼저)를 그대로 따르면 문제없다 — `docker compose --env-file .env -f ...`로도 우회 가능.

## DLQ 재처리 도구 도입 — 2026-09-18

`code/pipelines/ingestion/dlq_replayer.py`. 설계 배경과 사유 코드별 보정 가이드는
`docs/domain/data-contracts.md`의 "DLQ 재처리" 절 참고. 여기서는 실행 절차와 검증 결과만 기록한다.

### 조회·실행 절차

1. **조회(dry-run)**: 대상 DLQ 파티션·offset 범위·원본 topic을 정해 실행한다.

   ```bash
   PYTHONPATH=code python -m pipelines.ingestion.dlq_replayer \
     --dlq-partition 0 --from-offset 100 --to-offset 120 \
     --original-topic ecommerce.purchase --reason-code INVALID_PRICE
   ```

   `--execute` 없이 실행하면 재발행 없이 각 레코드의 통과/거부 여부만 보고서에 남는다.
2. 거부된 레코드 중 보정 가능한 것이 있으면 `corrections.jsonl`을 만든다(바뀔 필드만,
   `{"dlq_partition": 0, "dlq_offset": N, "corrections": {...}}` 한 줄씩 - `dlq_partition`이
   `--dlq-partition`과 다르면 실행 자체를 거부한다).
3. **실행**: 같은 명령에 `--corrections corrections.jsonl --execute`를 붙인다.
4. `reports/dlq-replay-<timestamp>.json`에서 `attempted/replayed/rejected/failed` 집계와
   레코드별 상세(원래 사유, 원본/재계산 event_id)를 확인한다. `failed`가 1건이라도 있으면
   프로세스가 종료 코드 1로 끝난다(자동화나 커맨드 결과만 봐도 실패를 놓치지 않게).

### 격리 토픽 통합 검증 — 2026-09-18 완료

`test.dlqreplayer.original`/`test.dlqreplayer.dlq`(1-partition, 실행 후 삭제)를 만들어
DLQ 스키마와 동일한 형태의 레코드 2건을 직접 발행해 검증했다 — 실제 view/cart/purchase나
운영 DLQ 토픽은 건드리지 않았다.

- offset 0: `INVALID_PRICE`(가격 음수) → `{"price": "12.50"}` 보정 제공 → 재검증 통과 →
  원본 토픽에 실제로 재발행됨을 컨슈머로 직접 확인. `event_id`가 보정 전과 다르게
  재계산됨도 확인
- offset 1: `MISSING_REQUIRED_FIELD`(user_id 빈 문자열) → 보정 미제공 → 거부
- dry-run 결과(재발행 없음)와 execute 결과(1건 재발행·1건 거부·0건 실패)가 정확히 일치
- 같은 corrections로 같은 범위를 다시 실행해도 event_id가 결정론적으로 동일하게
  재계산됨을 확인(단위 테스트로 커버) - status 저장소 없이도 이중 실행에 안전한 이유

### 리팩터: event_id 계산 로직 공유

`kafka_producer.py`의 `make_event_id`를 `event_contract.compute_event_id`로 옮겨 producer와
replayer가 동일한 해시 로직을 쓰게 했다. 두 곳이 각자 구현했다면 필드 순서나 None 처리가
미묘하게 갈라져도 알아채기 어려웠을 것이다.

### 코드 리뷰로 잡은 결함 4건 — 2026-09-18 완료

첫 구현을 코드 기준으로 다시 검토받아 실제 결함 4건을 발견하고 고쳤다. 전부 위 격리
토픽 통합 검증을 다시 돌려 재확인했다.

1. **`event_id` 재계산이 정규화 전 값을 해싱했다**: `compute_event_id`의 `str(v or "")`가
   `price=0`처럼 falsy하지만 유효한 값을 빈 문자열로 뭉갰다. 또 `process_record`가 검증
   *전* 원시값으로 해시를 계산해, `category_code`가 배열로 온 레코드는 검증기가 NULL로
   취급하는데 해시는 배열의 문자열 표현을 기준으로 계산되는 불일치가 있었다. 순서를
   "보정 적용 → 검증·정규화 → 정규화된 값으로 event_id 계산 → 정규화된 payload로 재구성"
   으로 바꾸고, `compute_event_id`도 `is None`만 빈 문자열로 처리하도록 고쳤다. 격리
   토픽에 `price: 0`(JSON number)인 레코드를 실제로 넣어 재검증 - 재발행된 payload의
   `price`가 `"0"`으로 정확히 보존됨을 확인했다.
2. **요청한 offset 범위를 다 못 읽어도 성공처럼 끝났다**: `consumer_timeout_ms` 안에 후속
   메시지가 없으면 순회가 조용히 끝나, `--to-offset`이 DLQ의 실제 끝보다 크면 일부만
   읽고 정상 리포트를 만들었다. `fetch_dlq_records`가 시작 전 `beginning_offsets`/
   `end_offsets`로, 끝난 뒤 `consumer.position()`으로 범위를 다 커버했는지 확인하고,
   못 채우면 `DlqRangeError`를 던져 재발행 자체를 안 하도록 고쳤다. 격리 토픽에서
   실제 끝(offset 2)보다 큰 `--to-offset=999`를 요청해 예외가 발생함을 확인했다.
3. **Kafka 발행 실패가 있어도 프로세스 종료 코드가 0이었다**: `main()`이 `failed` 건수를
   보지 않고 항상 정상 종료했다. `failed > 0`이면 `RuntimeError`를 던지도록 고쳤다
   (`rejected`는 검증 규칙대로 걸러진 정상 결과라 실패로 안 침). `producer.flush()`도
   예외가 나면 리포트 자체가 유실되지 않도록 try/except로 감쌌다(개별 발행은 이미
   `future.get()`으로 ACK를 확인해서 flush 실패가 성공 건수를 뒤집지는 않는다).
4. **보정 파일이 DLQ 파티션을 구분하지 않았다**: `corrections.jsonl`이 `dlq_offset`만으로
   레코드를 식별해, 다른 파티션(예: 파티션 1)에 있는 같은 offset 번호의 레코드와 섞일
   위험이 있었다. 보정 파일에 `dlq_partition` 필드를 추가하고 `--dlq-partition`과
   다르면 거부하도록 고쳤다. 중복된 `dlq_offset`도 조용히 덮어쓰지 않고 오류로 처리한다.

단위 테스트 11개를 추가해(정규화 순서, falsy 값, 범위 완전성 3종, 파티션 불일치, 중복
offset) 45개로 늘렸고, 문서(`data-contracts.md`)의 사유 코드 가이드와 "같은 범위를
두 번 실행해도 안전하다" 표현도 정정했다 - Silver의 논리적 정확성은 유지되지만 Bronze
물리적 중복·Flink 잠정 KPI 이중 집계·재처리 비용은 실제로 발생한다.
