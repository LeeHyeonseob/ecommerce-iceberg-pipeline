# 003. Silver MOR·Gold COW와 Spark 작업 직렬화

## 상태

확정 (2026-09-09)

## 배경

Silver는 증분 MERGE로 기존 행을 갱신하고, 특히 `silver_funnel`은 새 구매가 들어오면 과거 30일 범위의 퍼널까지 변경할 수 있다. Gold는 영향 날짜의 결과 전체를 원자적으로 교체한다. 증분 MERGE와 Iceberg 유지보수는 같은 파티션을 동시에 수정할 수 있다.

## 결정

- Silver 테이블은 Merge-on-Read를 사용한다.
- Gold 테이블은 Copy-on-Write를 유지한다.
- 증분 작업과 유지보수 작업은 Airflow `spark_pool` 1슬롯에서 직렬 실행한다.
- 유지보수는 재작성(`iceberg_compaction`)과 삭제(`iceberg_cleanup`) 두 DAG로 분리한다.
- snapshot 보존 기본값은 30일이다.

## 이유

- Silver MOR은 MERGE 때 영향 데이터 파일 전체를 매번 재작성하는 부담을 줄인다.
- Gold는 영향 날짜 전체 overwrite라 row-level delete 파일의 이점이 작고, COW가 BI 조회에 단순하다.
- 단일 로컬 Spark runner에서는 동시성보다 commit 충돌 회피와 예측 가능성이 중요하다.
- 재작성은 새 파일을 쓰고 기존 파일을 지우지 않아 되돌릴 수 있지만, 삭제는 되돌릴 수 없다. 위험도와 적정 주기가 다른 두 작업이 한 트리거에 묶이면 위험한 쪽에 맞춰 운영하게 된다.
- 풀 1슬롯은 상호 배제를 보장하지만 순서를 보장하지 않는다. Airflow 풀은 태스크 단위로 적용되므로 DAG 전체를 연속 구간으로 예약하지 않는다. 그래서 증분과 재작성은 `TriggerDagRunOperator`로 연결한다.
- Cross-session window와 snapshot retention은 서로 다른 정책이다. 다만 과거 30일 퍼널이 계속 변경될 수 있으므로, 변경 전후의 비교·복구 범위를 확보하기 위해 현재 보존 기간을 같은 30일로 맞췄다.

## 포기한 대안

- 전 테이블 COW: 읽기는 단순하지만 Silver MERGE의 재작성량이 커진다.
- 전 테이블 MOR: Gold에도 delete file 읽기·유지보수 비용이 생기지만 현재 overwrite 방식에서는 이점이 작다.
- Spark 작업 동시 실행: 현재 자원에서는 경합과 Iceberg commit 충돌 위험이 더 크다.

## 결과와 번복 조건

Silver의 delete 파일 수·크기와 Athena 조회 시간을 관측한다. 읽기 비용이 과도하면 compaction 임계값을 조정하거나 COW 복귀를 검토한다.

2026-09-13 `silver_funnel` 첫 compaction 실측: 기본 옵션으로 data file 78개가 재작성돼 83개에서 31개(파티션당 1개)로 줄었고 796,814,217 bytes를 55.7초에 처리했다. 사전 예측은 `min-input-files=5` 때문에 0건이었으나 목표 크기 미달이 선정 조건으로 작동했다. 따라서 data file 쪽은 옵션 조정 없이 기본값으로 운영한다. 다만 `rewrite_position_delete_files`가 0건을 반환해 dangling delete 52개가 메타데이터에 남았다. 상세는 [runbook](../operations/runbook.md)에 기록했다. 30일 이전 상태로 time travel이나 rollback이 필요하면 snapshot 보존 기간을 늘린다. 원본 기반 백필 가능 범위는 snapshot이 아니라 Bronze 보존 기간에 달려 있다.

## 구현 근거

- [Silver Events DDL](../../code/ddl/01_silver_events.sql)
- [Silver Funnel DDL](../../code/ddl/02_silver_funnel.sql)
- [기존 Silver MOR 마이그레이션](../../code/ddl/migrations/001_silver_to_mor.sql)
- [Iceberg 재작성 DAG](../../airflow/dags/iceberg_compaction.py)
- [Iceberg 삭제 DAG](../../airflow/dags/iceberg_cleanup.py)
- [Airflow Spark 작업 직렬화 설정](../../infra/docker-compose.airflow.yml)
