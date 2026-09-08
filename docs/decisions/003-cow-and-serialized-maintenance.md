# 003. COW 운영과 Spark 작업 직렬화

## 상태

확정 (날짜 미기록)

## 배경

현재 주요 소비자는 Athena/Superset 읽기이며, 증분 MERGE와 Iceberg 유지보수가 같은 파티션을 동시에 수정할 수 있다.

## 결정

- 운영 Silver/Gold 테이블은 Copy-on-Write를 사용한다.
- 증분 작업과 유지보수 작업은 Airflow `spark_pool` 1슬롯에서 직렬 실행한다.
- snapshot 보존 기본값은 30일이다.

## 이유

- COW는 읽을 때 delete file 병합이 없어 BI 쿼리에 유리하다.
- 단일 로컬 Spark runner에서는 동시성보다 commit 충돌 회피와 예측 가능성이 중요하다.
- Cross-session window와 snapshot retention은 서로 다른 정책이다. 다만 과거 30일 퍼널이 계속 변경될 수 있으므로, 변경 전후의 비교·복구 범위를 확보하기 위해 현재 보존 기간을 같은 30일로 맞췄다.

## 포기한 대안

- 전 테이블 MOR: 쓰기는 가벼워지지만 읽기 비용과 delete file 유지보수가 늘어난다.
- Spark 작업 동시 실행: 현재 자원에서는 경합과 Iceberg commit 충돌 위험이 더 크다.

## 결과와 번복 조건

쓰기 증폭이 배치 SLA를 반복해서 넘으면 `silver_funnel`부터 MOR을 검토한다. 30일 이전 상태로 time travel이나 rollback이 필요하면 snapshot 보존 기간을 늘린다. 원본 기반 백필 가능 범위는 snapshot이 아니라 Bronze 보존 기간에 달려 있다.
