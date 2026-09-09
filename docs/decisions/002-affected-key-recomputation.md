# 002. 영향 키 기반 퍼널 재계산

## 상태

확정 (날짜 미기록)

## 배경

늦게 도착한 view/cart는 기존 퍼널의 최초 시각을 바꾸고, 새 purchase는 같은 사용자·상품의 과거 다른 세션 퍼널을 바꿀 수 있다.

## 결정

- 직접 영향 키 `(user_session, product_id)`와 구매 전파 영향 키를 합쳐 재계산한다.
- 영향 키의 Silver 이벤트 전체 이력을 다시 읽는다.
- cross-session 전환은 anchor 이후 30일 purchase evidence로 다시 계산한다.
- 기존·신규 `funnel_date` 합집합으로 MERGE 파티션을 제한한다.

## 이유

- 배치 이벤트만 집계하면 늦은 이벤트가 기존 이력과 결합되지 않는다.
- 전체 Funnel 재구축은 데이터가 증가할수록 불필요한 읽기와 쓰기가 크다.
- 이미 전환된 행도 더 이른 지연 purchase로 `later_purchase_ts`가 바뀔 수 있다.

## 포기한 대안

- 매번 전체 재구축: 정확하지만 증분 처리의 비용 이점이 없다.
- 이번 배치의 직접 키만 갱신: 새 purchase가 과거 다른 세션에 미치는 영향을 놓친다.
- `converted_later=0`만 재검사: 더 이른 구매가 늦게 들어오는 경우를 놓친다.

## 결과와 번복 조건

전체 재구축과 증분 결과는 `updated_at`을 제외하고 같아야 한다. 100x에서 30일 evidence 조회가 병목이면 bucketing이나 별도 구매 근거 테이블을 검토한다.

## 구현 근거

- [Silver Funnel 재계산](../../code/pipelines/silver_events_to_funnel.py)
- [Airflow 증분 DAG](../../airflow/dags/ecommerce_incremental.py)
