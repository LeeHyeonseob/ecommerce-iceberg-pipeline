# 데이터 계약

| 데이터 | Grain / 논리 키 | 파티션 | 쓰기 방식 |
| --- | --- | --- | --- |
| Bronze raw zone | 이벤트 한 건 | 수집 시간 `raw_datetime` | Flink append-only Parquet |
| `silver_events` | 이벤트 한 건 / `event_id` | `event_date` | MOR, 영향 날짜 Iceberg MERGE |
| `silver_funnel` | `(user_session, product_id)` | `funnel_date` | MOR, 영향 키 전체 이력 재계산 후 MERGE |
| `gold_daily_gmv` | 일 | `summary_date` | 영향 날짜 overwrite |
| `gold_funnel_daily` | 일·카테고리 및 `ALL` | `summary_date` | 영향 날짜 overwrite |
| `gold_category_gmv` | 일·차원 종류·값 | `summary_date` | 영향 날짜 overwrite |
| `gold_pipeline_sla` | 일·시간·이벤트 타입 | `summary_date` | 영향 날짜 overwrite |
| `gold_data_quality` | 일 | `summary_date` | 두 날짜 축 합집합 overwrite |

정확한 컬럼·타입·순서는 `code/ddl/*.sql`이 기준이다. Spark의 `SILVER_COLUMNS`, `FUNNEL_COLUMNS`, `GOLD_COLUMNS`도 DDL과 같아야 한다.

## 이벤트 규칙

- 입력 event type은 `view`, `cart`, `purchase`다.
- `event_id`는 원본 9컬럼을 고정 순서로 연결한 SHA-256이다.
- 중복 `event_id`는 최신 `ingest_ts`, partition, 최신 offset 순으로 한 행을 남긴다.
- 빈 category와 brand는 NULL로 바꾸며 category code는 최대 3레벨로 분해한다.

## 입력 레코드 계약과 DLQ

Kafka → Bronze 진입 시 `code/pipelines/event_validation.py`의 `validate_event`가 검증하고,
`raw_zone_consumer.py`의 UDF가 결과를 PyFlink Row로 변환한다.
실패한 레코드는 Bronze에 넣지 않고 `ecommerce.events.dlq`(단일 토픽, 30일 retention)로 보낸다.

- 필수값: `event_id`, `event_type`, `event_time`, `user_id`, `user_session`, `product_id` (공백만 있는 문자열도 누락으로 취급)
- `event_type`은 `view`/`cart`/`purchase` 중 하나이며 수신한 topic과 일치해야 한다
- `event_time`은 `%Y-%m-%d %H:%M:%S %Z` 형식으로 파싱 가능해야 한다(producer와 동일 형식)
- `price`는 숫자로 파싱 가능·유한값·0 이상이어야 한다. JSON number와 숫자 문자열 모두 허용한다(producer가 CSV 문자열을 그대로 보내 JSON number가 아니기 때문)
- `category_code`, `brand`는 NULL을 허용한다
- 필수 필드가 JSON number/bool로 와도 문자열로 강제 변환해 허용하고, dict/list처럼 강제 변환이 의미를 바꾸는 타입은 누락으로 취급한다(Row 인코딩 단계에서 타입 불일치로 job이 죽는 것을 막기 위함)

reason code (닫힌 집합, `dlq_sink.reason_code`):

| reason_code | 의미 |
| --- | --- |
| `MALFORMED_JSON` | payload가 JSON으로 파싱 안 되거나 object가 아님 |
| `MISSING_REQUIRED_FIELD` | 필수 필드가 없거나 빈 문자열/공백 (`failure_detail`에 필드명) |
| `EVENT_TYPE_MISMATCH` | `event_type`이 view/cart/purchase가 아니거나 topic과 다름 |
| `INVALID_EVENT_TIME` | `event_time` 파싱 실패 |
| `INVALID_PRICE` | `price`가 숫자가 아니거나 음수·NaN·Infinity |
| `VALIDATION_INTERNAL_ERROR` | 검증 로직 자체의 예상 못한 예외 (never-throw 원칙의 최종 방어선) |

`failure_stage`는 항상 `FLINK_VALIDATION`이다(원본 topic은 `original_topic`에 별도 저장하므로 중복하지 않는다). 여러 검증 단계가 생기기 전까지는 이 값 하나만 쓴다.

## 퍼널 규칙

- `viewed`, `carted`, `purchased`는 같은 세션 내부 존재 여부다.
- 세션 내 구매가 있는 퍼널에는 `converted_later`를 붙이지 않는다.
- category와 brand는 현재 `max`로 대표한다. 이 선택을 바꾸면 과거 결과와 호환되지 않는다.

## KPI 규칙

- GMV와 purchase event count는 `silver_events`에서 계산한다.
- 전환율, 이탈률, cart value는 `silver_funnel`에서 계산한다.
- NULL 차원은 `unknown`으로 보존한다.
- `ALL`과 카테고리 행 또는 여러 `dim_type`을 함께 합산하지 않는다.
- 기간 비율은 일별 비율 평균이 아니라 `SUM(분자) / SUM(분모)`로 계산한다.
