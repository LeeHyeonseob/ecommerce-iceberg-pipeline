# 최종 프로젝트 — 이커머스 행동 로그 기반 Iceberg 데이터 플랫폼

## 1. 도메인 정의 + 데이터셋 + 핵심 KPI

### 1-1. 데이터셋

**선정: [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)** (Kaggle, 제공자 mkechinov / REES46 Marketing Platform)

대형 멀티카테고리 온라인 스토어의 실제 사용자 행동 이벤트 로그. `event_type`(view / cart / purchase), `event_time`(초 단위 실제 타임스탬프), `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id`, `user_session` 컬럼으로 구성. 전체 2019-10~2020-04(7개월, 약 2.85억 건) 중 **2019-10 한 달(4,245만 건, 일 평균 136.9만 건)** 을 로컬 데모 서브셋으로 사용

### 1-2. 가정 페르소나


| 이해관계자 | 요구사항                               |
| ----- | ---------------------------------- |
| 마케팅팀  | view -&gt; cart -&gt; purchase 전환율 |
| 상품팀   | 카테고리, 브랜드별 판매 변동                   |
| 경영진   | 일별 매출                              |
| 데이터   | 파이프라인 안정성 및 내구성                    |


### 1-3. 핵심 KPI

1. **일별 매출(GMV)** — `event_type = purchase`의 `price` 합
2. **퍼널 전환율** — view → cart → purchase 단계별 전환율 (user_session 기준 self-join)
3. **카테고리/브랜드별 매출 기여도**
4. **Cart 이탈률** — 1 − (cart→purchase 전환율)

---

## 2. 전체 아키텍처 (그림 + 설명)

*TODO — 로컬 Docker 스택(Redpanda/Spark/MinIO/Iceberg REST Catalog/Trino/Superset/Airflow) 구성 및 AWS 대응표 정리 예정*

## 3. 메달리온 3계층 의사결정

### 3-1. Bronze (raw)

*TODO*

### 3-2. Silver (processed)

*TODO*

### 3-3. Gold (summary)

*TODO*

## 4. 이 도메인에서 Iceberg가 가장 가치 있는 지점

*TODO — product 디멘션 SCD(MERGE), 컴팩션-스트리밍 OCC 충돌, 백필 시나리오 등 후보 정리 예정*

## 5. 운영 헬스 체크 쿼리 모음

*TODO*

## 6. 대시보드 (스크린샷 + 운영 메트릭)

*TODO*

## 7. 100x 스케일 아웃 시나리오 (설계만, 구현 X)

*TODO*

## 8. 장애·운영 시나리오

*TODO*

## 9. 멱등성 / 재처리 가능성 설계

*TODO*