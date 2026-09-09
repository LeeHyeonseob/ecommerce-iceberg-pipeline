# 001. 잘못된 Gold 집계 기준

## 시도한 날짜

날짜 미기록

## 시도한 이유

퍼널 테이블 하나에서 GMV와 전환 지표를 함께 계산하고, 간단한 집계식으로 카테고리 롤업과 비율을 만들려고 했다.

## 실패 증상

- Funnel에서 GMV를 계산하면 동일 세션·상품의 반복 구매가 접혀 7.1% 과소집계됐다.
- `purchases / carts`를 Cart→Purchase 전환율로 쓰면 cart를 거치지 않은 구매가 섞여 110%가 됐다.
- NULL category 처리 전에 rollup을 구분하면 category 결측 33.88%가 `ALL`로 보였다.

## 실패 원인

- 이벤트와 퍼널의 grain을 혼합했다.
- 비율 분자가 분모의 부분집합이 아니었다.
- 실제 NULL과 grouping set의 집계 NULL을 구분하지 않았다.

## 현재 대안

- GMV는 `silver_events` purchase에서 계산한다.
- Cart→Purchase는 `carted=1 AND purchased=1`을 분자로 사용한다.
- category NULL을 `unknown`으로 바꾼 뒤 `grouping()`으로 `ALL`을 구분한다.

## 에이전트 지침

Gold 지표를 수정할 때 데이터 grain과 분자·분모 포함 관계를 먼저 검증한다. Funnel에서 GMV를 계산하거나 NULL과 `ALL`을 합치지 않는다.

## 구현 근거

- [Gold 집계 로직](../../code/pipelines/silver_to_gold.py)
- [Gold Funnel DDL](../../code/ddl/04_gold_funnel_daily.sql)
