# 001. Bronze Parquet와 Silver·Gold Iceberg 분리

## 상태

확정 (날짜 미기록)

## 배경

수집 원본은 변형 없이 보존해야 하고, 정제·퍼널·집계 데이터는 지연 도착으로 과거 결과가 바뀐다.

## 결정

- Bronze는 Kafka 메타데이터를 포함한 append-only plain Parquet로 저장한다.
- Silver와 Gold는 Glue Catalog의 Iceberg format v2 테이블로 저장한다.

## 이유

- Bronze는 단순 수집과 원본 재처리가 목적이라 갱신 기능이 필요하지 않다.
- Silver/Gold는 MERGE, 원자적 파티션 교체, snapshot과 metadata table이 필요하다.
- plain Parquet만 쓰면 파일 교체, 동시 쓰기, 실패 복구와 버전 보존을 직접 구현해야 한다.

## 포기한 대안

- 전 계층 plain Parquet: 과거 데이터 갱신과 원자성 구현 부담이 크다.
- Bronze까지 Iceberg: 현재 append-only 수집 요구에 비해 카탈로그와 commit 복잡도가 늘어난다.

## 결과와 번복 조건

Bronze는 Iceberg compaction을 사용할 수 없다. Bronze 파일 수가 sink 조정으로 관리되지 않을 때 별도 Parquet 병합 또는 Bronze Iceberg 전환을 다시 검토한다.
