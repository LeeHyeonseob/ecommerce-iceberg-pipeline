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

Kafka → Bronze 진입 시 `code/pipelines/common/event_validation.py`의 `validate_event`가 검증하고,
`pipelines/ingestion/raw_zone_consumer.py`의 UDF가 결과를 PyFlink Row로 변환한다.
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

## DLQ 재처리

`code/pipelines/ingestion/dlq_replayer.py`가 DLQ에 격리된 레코드를 재검증해 원본 topic으로
재발행하는 운영자용 CLI다. 자동 스케줄 작업이 아니며, 사람이 DLQ 파티션 1개와 `[from, to)`
offset 범위 1개를 지정해 매번 명시적으로 실행한다(DLQ topic은 3-partition이라 여러 파티션은
여러 번 실행). 기본은 `--dry-run`이고 `--execute`를 붙여야 실제 재발행한다.

- 원본 위치(`dlq_sink.kafka_partition`/`kafka_offset`, 즉 실패 당시 view/cart/purchase
  좌표)와 DLQ 위치(DLQ topic 자체의 partition·offset, Kafka 컨슈머가 주는 값)는 서로 다른
  개념이다. 재처리 대상 식별·중복 방지의 기준은 후자(`dlq_offset`)다.
- 보정은 전체 payload 재입력이 아니라 **바뀔 필드만** JSONL로 준다(`{"dlq_partition": 0,
  "dlq_offset": N, "corrections": {"price": "12.50"}}`). `dlq_partition`은 이번 실행의
  `--dlq-partition`과 반드시 일치해야 한다(다른 파티션은 offset이 독립적이라, 안 맞으면
  엉뚱한 레코드에 보정이 적용될 수 있어 값이 다르면 거부한다). 도구 자체는 값을 추측해서
  채우지 않는다 — 사람이 실제 근거가 있는 값만 넣는다는 전제다. 사유 코드별 보정 가능 여부 가이드:

  | reason_code | 보정 판단 |
  | --- | --- |
  | `INVALID_EVENT_TIME` | 대체로 안전 — 포맷 문제일 가능성이 높음 |
  | `EVENT_TYPE_MISMATCH` | 원본 topic이 맞고 payload의 `event_type` 필드만 잘못됐다는 근거가 있을 때만. 이 도구는 항상 원래 실패했던 topic으로 재발행하므로 topic 자체가 틀렸던 경우(다른 topic으로 다시 라우팅)는 범위 밖 — 그 경우는 수동으로 처리한다 |
  | `INVALID_PRICE` | 명백한 오타(부호·구분자)면 보정, 원래 값을 모르면 거부 |
  | `MISSING_REQUIRED_FIELD` | 신원 필드(`user_id` 등)는 다른 데이터로 실제 확인 가능할 때만 |
  | `MALFORMED_JSON` | 원문 복원이 아니라 추측에 가까워 대체로 거부 |
  | `VALIDATION_INTERNAL_ERROR` | 데이터가 아니라 검증 로직 버그일 가능성 — 코드 수정 대상 |

- 보정 여부와 무관하게 재처리 대상은 항상 `event_id`를 **검증·정규화가 끝난 값**(문자열 강제
  변환, dict/list → NULL 등 `validate_event`가 실제로 적용하는 규칙) 기준으로 재계산한다.
  보정 적용 → 검증·정규화 → 정규화된 9개 필드로 `event_id` 계산 → 정규화된 payload 재구성
  → 발행 순서를 지킨다. 검증 전 원시값을 그대로 해싱하면 예를 들어 `category_code`가 배열로
  온 레코드는 검증기가 NULL로 취급하는데 `event_id`는 배열의 문자열 표현으로 계산돼 내용과
  안 맞게 된다. `event_id = 내용의 해시`라는 불변조건을 유지하기 위함이며, 보정 없이 원문
  그대로 재검증하는 경우도 예외 없이 재계산한다(드리프트 감지 겸용).
- 재계산은 순수 함수라 같은 입력을 두 번 재처리해도 같은 `event_id`가 나온다 — 그래서
  **Silver 결과의 논리적 정확성(멱등성)은** 실수로 같은 범위를 두 번 실행해도 유지된다.
  다만 이게 "완전히 안전"하다는 뜻은 아니다: Bronze는 append-only라 물리적으로 중복
  레코드가 쌓이고, Flink 실시간 잠정 KPI는 재발행 건을 다시 집계하며, Kafka·Flink 처리
  비용도 다시 발생한다. 그래서 이중 실행 자체를 막는 안전장치(파티션·offset 범위 완전성
  검사, 보정 파일의 `dlq_partition` 일치 검사)를 별도로 둔다.
- 별도의 "재처리 완료" 상태 저장소(예: compacted Kafka topic)는 두지 않는다. "재발행"과
  "상태 기록"을 원자적으로 묶을 수 없어 완벽한 중복 방지가 되지 않기 때문이다. 대신 실행마다
  `reports/dlq-replay-<timestamp>.json`에 상세 감사 기록을 남기고, 최종 중복 방어는 위
  event_id 재계산의 결정론적 성질과 Silver MERGE에 맡긴다.

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
