# 이커머스 행동 로그 기반 Iceberg Lakehouse

Kafka로 재생한 이커머스 행동 로그를 Flink로 S3 Bronze에 적재하고, Spark·Iceberg로 Silver/Gold를 구성한 뒤 Athena와 Superset으로 분석하는 데이터 플랫폼입니다.

핵심 문제는 지연 도착 이벤트와 세션을 넘는 구매가 이미 계산한 과거 퍼널을 바꾼다는 점입니다. 영향 키 기반 재계산과 Iceberg의 원자적 갱신으로 이를 처리합니다.

## 1. 도메인·데이터 범위·핵심 KPI

데이터셋은 Kaggle의 [eCommerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)다. 전체 7개월 약 2.85억 건 중 현재는 **2019년 10월 4,245만 건**을 사용한다.

| KPI | 정의 | 대상 |
| --- | --- | --- |
| GMV | `purchase` 이벤트의 `price` 합 | 경영진 |
| 퍼널 전환율 | `(user_session, product_id)`별 view·cart·purchase 존재 여부 | 마케팅 |
| Cart 이탈률·미전환 금액 | cart 후 구매로 연결되지 않은 비율과 금액 | 마케팅 |
| 카테고리 GMV | 카테고리별 GMV 기여도 | 상품팀 |
| 운영 품질 | 지연, NULL, 가격 이상치, Iceberg 테이블 상태 | 데이터팀 |

`order_id`, `cart_id`, `quantity`가 없어 실제 주문 단위는 복원할 수 없다. 따라서 구매 건수 대신 **purchase 이벤트 수**로 표기한다.

## 2. 전체 아키텍처

![데이터 파이프라인 아키텍처](assets/ecommerce_data_pipeline_diagram.png)

```text
CSV gzip → Kafka (view / cart / purchase) → Flink → S3 Bronze Parquet
         → Spark + Iceberg Silver → Spark + Iceberg Gold → Athena → Superset
```

| 구간 | 기술 | 책임 |
| --- | --- | --- |
| Producer·Kafka | Python, Kafka | `event_type`별 토픽으로 이벤트 재생, `user_id` 키 분배 |
| Flink | Flink Table API | 토픽별 독립 소비, 체크포인트 기반 Bronze 적재 |
| Bronze | S3 plain Parquet | 수집 메타데이터를 포함한 append-only 원본 보존 |
| Silver·Gold | Spark, Iceberg, Glue Catalog | 정제·MERGE·퍼널 조립·KPI 집계 |
| Query·BI | Athena, Superset | 서버리스 조회와 대시보드 |
| Orchestration | Airflow | 증분 순서·재시도·Spark 작업 직렬화 |

현재는 **로컬 Docker Compose 재현 환경**이다. 저장소는 AWS S3, 카탈로그는 AWS Glue Catalog, 조회 엔진은 Athena를 사용하며 Airflow·Superset·PostgreSQL·Redis는 컨테이너로 실행한다.

## 3. 메달리온 계층과 데이터 계약

| 계층 | 테이블 | Grain | 책임 |
| --- | --- | --- | --- |
| Bronze | `raw/{view,cart,purchase}` | 이벤트 1건 | 원본·수집 메타데이터 보존 |
| Silver | `silver_events` | 이벤트 1건 | dedup, 타입 변환, 카테고리 분해 |
| Silver | `silver_funnel` | `(user_session, product_id)` | 행동 여정과 cross-session 전환 |
| Gold | `gold_daily_gmv` | 일 | GMV와 purchase 이벤트 수 |
| Gold | `gold_funnel_daily` | 일·카테고리 | 전환율, Cart 이탈, 미전환 금액 |
| Gold | `gold_category_gmv` | 일·차원 | 카테고리·브랜드·상품 기여도 |
| Gold | `gold_pipeline_sla` | 일 | 처리 지연 |
| Gold | `gold_data_quality` | 일 | NULL, 가격, 행동 품질 |

GMV는 `silver_events`, 전환율·이탈률은 `silver_funnel`에서 계산한다. 서로 다른 grain의 테이블을 함께 합산하지 않는다.

### Cross-session 전환과 해석 제약

어떤 세션에서 상품을 보고 구매하지 않았더라도 같은 사용자가 다른 세션에서 같은 상품을 구매하면, 과거 퍼널에 전환을 표시한다. 탐지 기간은 30일이다.

- `converted_later`: 다른 세션의 후속 구매 여부
- `later_purchase_ts`: 가장 이른 후속 구매 시각
- `later_purchase_gap_sec`: 퍼널 anchor부터 후속 구매까지의 시간

7일 윈도우는 340,535건, 30일 윈도우는 394,493건의 지연 전환을 포착했다. 데이터 관측 기간이 31일뿐이므로 Cart 이탈률은 상한, 지연 전환 수는 관측 가능한 하한으로 해석한다.

## 4. 이 도메인에서 Iceberg가 필요한 이유

지연 도착한 view/cart 이벤트는 기존 퍼널의 최초 시각·집계값을 바꿀 수 있고, 새 purchase는 과거 다른 세션 퍼널의 `converted_later`를 바꿀 수 있다. plain Parquet만으로는 대상 파일 교체, 중간 실패 복구, 이전 버전 보존을 직접 구현해야 한다.

Iceberg의 `MERGE`, 조건부 파티션 교체, snapshot으로 이를 처리한다.

| 검증 항목 | 결과 |
| --- | --- |
| COW 재작성 비용 | 한 컬럼 UPDATE에 1,329,334행 / 105~118MB 재작성 |
| 파티션 가지치기 | 하루 2.28MB vs 전체 31일 78.3MB, 약 34배 차이 |
| 지연 전환 | 30일 기준 394,493건 |

현재 운영 테이블은 COW(Copy-on-Write)다. BI 읽기에 유리하지만 갱신 시 파일을 다시 쓰므로, 쓰기 증폭이 커지면 MOR(Merge-on-Read) 전환을 검토한다. snapshot은 cross-session 윈도우와 같은 30일을 보관 기준으로 둔다.

## 5. 증분 처리·멱등성·재처리

Airflow 실행은 `[from, to)` 수집 구간을 Bronze→Silver로 반영한다. 대량 영향 키는 XCom에 넣지 않고 S3 배치 산출물에 저장하며, XCom에는 그 경로만 전달한다.

```text
Bronze 구간 읽기
  → silver_events MERGE
  → 이번 배치의 키를 S3 산출물로 저장
  → 직접 영향 키 + 구매로 파급된 과거 퍼널 키 수집
  → 영향 키의 이벤트 이력 재조회
  → silver_funnel 재계산·MERGE
  → 변경 event_date / funnel_date의 Gold 재집계
```

- **직접 영향 키**: 이번 배치에서 바뀐 `(user_session, product_id)`
- **파급 영향 키**: 새 purchase가 같은 `(user_id, product_id)`의 과거 30일 퍼널에 미치는 영향

두 집합을 중복 제거해 같은 재계산 경로에 태운다. `converted_later=1` 행도 제외하지 않는다. 더 이른 지연 purchase가 뒤늦게 도착하면 `later_purchase_ts`가 다시 바뀔 수 있기 때문이다.

`silver_events` MERGE에는 배치의 `event_date`, `silver_funnel` MERGE에는 재계산 전·후 `funnel_date` 합집합을 조건으로 넣어 파티션 가지치기를 사용한다. Gold 증분은 영향 날짜를 `overwrite(predicate)`로 원자적으로 교체하며, 전체 재구축은 `overwritePartitions()`를 사용한다.

| 대상 | 멱등 방식 |
| --- | --- |
| `silver_events` | `event_id` MERGE |
| `silver_funnel` | `(user_session, product_id)` MERGE |
| Gold 5개 | 영향 날짜의 완전 재집계·원자적 파티션 교체 |

전체 재구축 모드는 별도로 유지한다. 검증 테이블에서 증분·전체 재구축 결과는 13,424,825행, 양방향 차집합 0건으로 일치했다(`updated_at` 제외).

## 6. Airflow 오케스트레이션과 Iceberg 유지보수

`ecommerce_incremental` DAG는 다음 순서로 실행된다.

```text
silver_events → silver_funnel → Gold 5개 → health_check
```

Airflow의 Bash Task는 `spark-runner` 컨테이너에 Spark batch를 제출한다. `spark_pool` 슬롯을 1개로 설정해 증분 MERGE와 유지보수가 같은 파티션을 동시에 갱신하지 않도록 직렬화한다.

별도 `iceberg_maintenance` DAG는 테이블별로 다음 순서를 실행한다.

```text
rewrite_data_files → rewrite_manifests → expire_snapshots → remove_orphan_files
```

현재 테이블은 COW이므로 MOR position delete 파일이 없어 `rewrite_position_delete_files`는 실행하지 않는다. 컴팩션과 MERGE의 충돌·복구 방식은 9절에서 다룬다.

## 7. 운영 가시성: 5분 헬스체크

운영자는 증분 DAG 마지막 태스크의 로그와 Superset 운영 탭에서 최신 파티션·파일 상태·품질 지표를 확인한다. `code/health-queries/`의 Iceberg 메타테이블 기반 쿼리는 7개다.

| 영역 | 확인 항목 |
| --- | --- |
| Silver·Gold | 최신 파티션, 파일 수·평균 크기·small file 수 |
| Iceberg snapshots | 최신 커밋 시각, snapshot 누적 수 |
| Iceberg manifests | manifest 수 |
| Iceberg history | HEAD 전환 시각, 현재 계보 밖 snapshot 수 |

`health_check.py`가 쿼리를 한 Spark 세션에서 실행한다. 현재는 결과를 로그로 남기는 수준이며, 정상 기준선과 알림 대상이 정해지면 임계값 기반 실패·알림을 추가한다.

Bronze는 plain Parquet이므로 Iceberg 메타테이블 기반 점검 대상이 아니다. Bronze 파일 크기와 도착 지연은 `verify_raw_zones.py`로 별도 진단한다.

## 8. Superset 대시보드

- **조회 엔진**: Athena
- **메타데이터 저장소**: PostgreSQL 16
- **필터·Athena 쿼리 결과 캐시**: Redis 7.2
- **대시보드 정의**: 데이터베이스 연결·데이터셋·차트·탭 배치·기간 필터를 포함한 공식 YAML export를 [`dashboard/superset`](dashboard/superset)에 보관

![Superset 비즈니스 KPI 탭](assets/superset_business_kpi.png)

![Superset 운영 품질 탭](assets/superset_operations.png)

- **비즈니스 KPI**: GMV, purchase 이벤트 수, Cart→Purchase 전환율, 이탈률, 카테고리 GMV, 미전환 금액
- **운영 품질**: 일별 행 수, NULL 비율, 가격 이상치, 처리 지연, Iceberg 파일 상태

`ALL` 행과 카테고리 행, 또는 여러 `dim_type`을 함께 합산하면 값이 중복된다. 이를 가상 데이터셋 필터로 차단하고 비율은 일별 비율의 평균 대신 `SUM(분자) / SUM(분모)`로 계산한다. 원본에 통화 정보가 없어 금액에 통화 기호를 붙이지 않았다.

## 9. 실행 방법

### 9.1 인프라 실행

`.env.example`을 복사해 AWS 자격증명, S3 버킷, Airflow·Superset 설정을 채운다. Compose 파일은 수집, 배치, BI 단위로 분리했다.

```bash
cp .env.example .env
set -a; source .env; set +a

# Kafka + Flink
docker compose -f infra/docker-compose.yml up -d --build

# Airflow + spark-runner
docker compose -f infra/docker-compose.airflow.yml up -d --build

# Superset + PostgreSQL + Redis
docker compose -f infra/docker-compose.superset.yml up -d --build
```

- Flink UI: `http://localhost:8081`
- Airflow UI: `http://localhost:8080`
- Superset UI: `http://localhost:8088`

### 9.2 Iceberg DDL 생성

최초 1회, `spark-runner`에서 Silver·Gold Iceberg 테이블을 생성한다.

```bash
docker exec spark-runner python /opt/project/code/pipelines/run_ddl.py
```

### 9.3 수집과 배치 실행

Flink Bronze 수집 잡을 제출한 뒤 Producer로 이벤트를 재생한다. 이후 Airflow UI에서 `ecommerce_incremental` DAG를 수동 실행한다.

```bash
for zone in view cart purchase; do
  docker exec jobmanager /opt/flink/bin/flink run -d -m jobmanager:8081 \
    -py /opt/flink/jobs/raw_zone_consumer.py \
    --topic "ecommerce.${zone}" --raw-path "s3://${S3_BUCKET}/raw/${zone}/"
done

python code/pipelines/kafka_producer.py --csv-path <csv_gzip_path> --speed 60
```

Airflow DAG는 `silver_events → silver_funnel → Gold 5개 → health_check` 순서로 실행한다.
