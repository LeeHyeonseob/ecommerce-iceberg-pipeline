# Agent guide

이 저장소는 Kafka → Flink → S3 Bronze → Spark/Iceberg Silver·Gold → Athena/Superset으로 이어지는 이커머스 행동 로그 파이프라인이다. 작업 전 [지식 저장소](docs/README.md)에서 변경 영역에 해당하는 문서를 읽는다.

## 반드시 지킬 불변 조건

- 원본에는 `order_id`, `quantity`, 통화가 없다. `purchase_cnt`를 주문 수로 부르거나 금액에 통화를 추정하지 않는다.
- GMV는 이벤트 grain인 `silver_events`의 `purchase`에서 계산한다. 퍼널에서 계산하면 반복 구매가 접혀 과소집계된다.
- `silver_events`의 논리 키는 `event_id`, `silver_funnel`의 논리 키는 `(user_session, product_id)`다.
- 퍼널의 cross-session 전환 창은 anchor 시각부터 30일이다. 새 구매는 과거 퍼널도 변경할 수 있다.
- 증분 퍼널은 직접 영향 키와 구매 전파 영향 키를 모두 재계산한다. 이미 `converted_later=1`인 행도 제외하지 않는다.
- Silver MERGE에는 영향 파티션 조건을 유지하고, Gold 증분은 영향 날짜의 전체 결과를 원자적으로 교체한다.
- `event_date`와 `funnel_date`는 서로 다른 날짜 축이다. 두 날짜 집합을 임의로 하나로 취급하지 않는다.
- Gold의 `ALL`과 카테고리 행, 또는 서로 다른 `dim_type`을 함께 합산하지 않는다. 기간 비율은 일별 비율의 평균이 아니라 분자·분모 합계로 다시 계산한다.
- Airflow가 파싱하는 Spark 작업의 마지막 출력 줄은 JSON이어야 한다. 대량 배치 데이터는 XCom이 아니라 S3 Parquet로 전달한다.
- Spark/Iceberg 작업은 `spark_pool` 1슬롯 직렬화를 유지한다.
- DDL 컬럼을 바꾸면 파이프라인의 컬럼 순서 상수와 Superset 데이터셋·차트까지 함께 점검한다.

## 작업 원칙

- `README.md`는 사용·설명 문서, `docs/`는 결정·실패·도메인 지식 저장소다. 같은 설명을 장황하게 복제하지 않는다.
- 프로젝트 코드와 문서가 다르면 코드를 현재 동작의 근거로 삼고 불일치를 명시한다.
- AWS 자격증명, `.env`, 실제 버킷명과 비밀값을 커밋하지 않는다.
- 변경 범위에 맞춰 최소한 Python 문법 검사와 DAG import 가능성을 확인한다. 실제 통합 검증에는 Docker, AWS S3, Glue 권한이 필요하다.

## 참고 문서

- 기술 결정: [docs/decisions](docs/decisions/) — 구조를 선택한 이유와 번복 조건
- 실패 기록: [docs/failures](docs/failures/) — 반복하면 안 되는 접근과 현재 대안
- 도메인 지식: [docs/domain](docs/domain/) — 용어, 데이터 계약, 업무 흐름과 한계
- 개발 규칙: [docs/conventions](docs/conventions/) — 변경 영향과 검증 기준
- 운영 지식: [docs/operations](docs/operations/) — 실행, 진단, 장애 대응
