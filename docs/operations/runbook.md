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

### 이후 옵션 정책

data file은 **기본 옵션을 유지한다.** 실측에서 목표 크기 미달이 선정 조건으로 작동해
파티션당 3개가 전부 재작성됐다. `min-input-files` 조정이 필요하지 않다.

position delete는 기본 옵션(`min-input-files=5`)으로는 dangling delete가 정리되지 않는다.
정기 실행에서 delete file이 누적되는 것이 관측되면 `delete_rewrite_options=min-input-files=2`를
검토한다. 상시 적용 여부는 일별 누적 속도를 본 뒤 정한다.

옵션은 한 번에 하나씩만 바꾼다. `delete-file-threshold=1`의 상시 적용은 MOR의 쓰기 절감
효과를 잃으므로 쓰지 않는다. `docker exec` 직접 실행은 `spark_pool`을 우회해 증분과 겹칠 수
있으므로 쓰지 않는다.
