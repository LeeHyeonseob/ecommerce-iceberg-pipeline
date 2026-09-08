# 데이터 계약

| 데이터 | Grain / 논리 키 | 파티션 | 쓰기 방식 |
| --- | --- | --- | --- |
| Bronze raw zone | 이벤트 한 건 | 수집 시간 `raw_datetime` | Flink append-only Parquet |
| `silver_events` | 이벤트 한 건 / `event_id` | `event_date` | 영향 날짜 Iceberg MERGE |
| `silver_funnel` | `(user_session, product_id)` | `funnel_date` | 영향 키 전체 이력 재계산 후 MERGE |
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
