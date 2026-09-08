# 주요 데이터 흐름

## 수집과 정제

```text
CSV → event type별 Kafka topic → Flink → S3 Bronze
→ 타입 정제·event_id dedup → silver_events MERGE
```

Bronze 증분 구간은 수집 시각 기준 `[from, to)`다. 후보 `raw_datetime` 파티션을 먼저 줄이고 `ingest_ts`로 경계를 정확히 적용한다. Silver MERGE 성공 후에만 Funnel용 S3 배치 산출물을 쓴다.

## 퍼널 증분

1. 이번 배치의 `(user_session, product_id)`를 직접 영향 키로 잡는다.
2. 신규 purchase가 바꿀 수 있는 과거 30일 퍼널을 전파 영향 키로 찾는다.
3. 두 키 집합의 Silver 이벤트 전체 이력을 다시 읽는다.
4. 30일 purchase evidence로 cross-session 전환을 다시 계산한다.
5. 기존·신규 `funnel_date` 합집합을 조건으로 Funnel을 MERGE한다.

이미 `converted_later=1`인 행도 더 이른 지연 purchase로 시간이 바뀔 수 있으므로 제외하지 않는다.

## Gold 증분

- 이벤트 기반 Gold는 `event_dates`를 사용한다.
- 퍼널 기반 Gold는 `funnel_dates`를 사용한다.
- Data Quality는 두 날짜 집합의 합집합을 사용한다.
- 선택 날짜를 완전히 재집계해 원자적으로 교체한다.

## Airflow 계약

```text
silver_events → silver_funnel → gold → health_check
```

Spark 로그의 마지막 JSON 한 줄을 Airflow가 파싱한다. 대량 배치 레코드는 XCom이 아니라 S3 Parquet로 전달하며, 빈 배치는 후속 Funnel과 Gold를 건너뛴다.
