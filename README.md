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
| 전환 전 COW 재작성 비용 | 한 컬럼 UPDATE에 1,329,334행 / 105~118MB 재작성 |
| 파티션 가지치기 | 하루 2.28MB vs 전체 31일 78.3MB, 약 34배 차이 |
| 지연 전환 | 30일 기준 394,493건 |

이 재작성 비용과 30일 퍼널 갱신 범위를 근거로 Silver는 MOR(Merge-on-Read)로 전환하고, 날짜 전체를 교체하는 Gold는 COW(Copy-on-Write)를 유지한다. snapshot은 cross-session 윈도우와 같은 30일을 보관 기준으로 둔다.

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

MOR 전환 후 필요한 유지보수 순서는 다음과 같다.

```text
iceberg_compaction  주 1회   rewrite_data_files → rewrite_position_delete_files   Silver 2개
iceberg_cleanup     주 1회   expire_snapshots → remove_orphan_files               7개 전부
```

재작성은 기존 파일을 즉시 물리 삭제하지 않고 새 snapshot을 만들므로 보존 기간 안에는 이전 상태를 조회할 수 있다. 삭제는 파일을 실제로 지우며 그 이후로는 되돌릴 수 없다. 위험도와 적정 주기가 달라 DAG를 나눴다. 재작성만으로는 저장 공간이 줄지 않으며, 공간은 `expire_snapshots`가 스냅샷을 만료시킬 때 회수된다.

Gold는 COW overwrite로 파티션당 data file이 1개라 `rewrite_data_files`가 구조적으로 0건이어서 재작성 대상에서 뺀다. `rewrite_manifests`는 쓰기 시 자동 병합(`commit.manifest-merge.enabled` 기본 true)이 있어 정기 실행하지 않고 `--steps rewrite_manifests`로 필요할 때 돌린다. 실행할 단계는 `--steps`로 고르되 순서는 정의 순서로 고정된다. 삭제는 `--as-of`로 기준 시각을 고정해 재시도마다 범위가 넓어지지 않게 한다. 증분과 재작성의 순서는 `TriggerDagRunOperator`로 연결한다 — Airflow 풀은 태스크 단위라 cron 시간차로는 보장되지 않는다. 주기는 관측 후 조정할 초기값이다.

## 7. 운영 가시성: 5분 헬스체크

운영자는 증분 DAG 마지막 태스크의 로그와 Superset 운영 탭에서 최신 파티션·파일 상태·품질 지표를 확인한다. `code/health-queries/`에는 다음 쿼리를 보관한다.

- Silver freshness: 최신 이벤트 날짜
- Silver file health: data 파일 수·크기와 목표 크기(128MB) 미달 수, MOR의 position delete 파일·레코드 수
- Gold freshness: 테이블별 최신 집계 날짜
- Gold file health: 파티션 수, data file이 2개 이상인 파티션 수, 파티션 최대 파일 수, 평균 크기
- Snapshot health: `snapshots` 메타테이블의 최신 커밋 시각·누적 수
- Manifest health: `manifests` 메타테이블의 manifest 수
- History health: `history` 메타테이블의 HEAD 전환 시각·현재 계보 밖 snapshot 수
- 파티션별 상세(`detail/`): Silver·Gold의 파티션 단위 파일 상태. `--detail`을 줬을 때만 실행

Gold는 일별 집계라 파일이 언제나 128MB에 한참 못 미친다. 크기 임계값이 신호가 되지 못하므로 Gold는 파티션당 파일 수를 compaction 신호로 쓴다. Silver는 `write.target-file-size-bytes`가 128MB로 지정돼 있어 미달 수가 의미를 갖는다.

`health_check.py`가 쿼리를 한 Spark 세션에서 실행한다. 기본 실행은 테이블 요약만 포함하고, `--detail`을 지정하면 `detail/` 디렉터리의 파티션별 파일 상태까지 출력한다. 상세 쿼리는 metadata 스캔이 커지므로 `--detail-tables`와 `--detail-partitions`(기본 14)로 대상 테이블과 최근 파티션 수를 제한한다. 현재는 결과를 로그로 남기는 수준이며, 정상 기준선과 알림 대상이 정해지면 임계값 기반 실패·알림을 추가한다.

Bronze는 plain Parquet이므로 Iceberg 메타테이블 기반 점검 대상이 아니다. Bronze 파일 크기와 도착 지연은 `verify_raw_zones.py`로 별도 진단한다.

실시간 시스템 상태는 Prometheus가 15초마다 수집한다. Flink reporter에서 처리량·backpressure·checkpoint를, Kafka exporter에서 consumer group·partition lag를, Kafka JVM의 JMX Exporter에서 브로커 내부 지표를 가져온다. 이 지표는 즉시 장애를 찾기 위한 것이고, 위 헬스 쿼리는 배치 커밋 후 데이터 상태를 검증하기 위한 것이므로 역할이 다르다.

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

# Prometheus + Kafka consumer-lag exporter + Grafana (Kafka/Flink 기동 후)
docker compose -f infra/docker-compose.monitoring.yml up -d

# Airflow + spark-runner
docker compose -f infra/docker-compose.airflow.yml up -d --build

# Superset + PostgreSQL + Redis
docker compose -f infra/docker-compose.superset.yml up -d --build
```

- Flink UI: `http://localhost:8081`
- Prometheus UI: `http://localhost:9090`
- Grafana 운영 대시보드: `http://localhost:3000` (로컬 anonymous viewer)
- Airflow UI: `http://localhost:8080`
- Superset UI: `http://localhost:8088`

### 9.2 Superset 대시보드 가져오기

새 환경에서는 관리자 계정을 만든 뒤 저장소의 Superset export를 ZIP으로 묶어 가져온다.

```bash
docker exec superset superset fab create-admin \
  --username admin --firstname Admin --lastname User \
  --email admin@example.com --password 'CHANGE_ME'

(cd dashboard/superset && zip -r ../superset-dashboard.zip .)
```

`http://localhost:8088`에 로그인해 **Settings → Import Dashboards**에서
`dashboard/superset-dashboard.zip`을 가져온다. 가져온 Athena Database 연결의 region,
workgroup, `s3_staging_dir`는 자신의 AWS 환경에 맞게 확인한다.

### 9.3 Iceberg DDL 생성

최초 1회, `spark-runner`에서 Silver·Gold Iceberg 테이블을 생성한다.

```bash
docker exec spark-runner python /opt/project/code/pipelines/operations/run_ddl.py
```

### 9.4 수집과 배치 실행

Flink Bronze 수집 잡을 제출한 뒤 Producer로 이벤트를 재생한다. 이후 Airflow UI에서 `ecommerce_incremental` DAG를 수동 실행한다.

```bash
for zone in view cart purchase; do
  docker exec jobmanager /opt/flink/bin/flink run -d -m jobmanager:8081 \
    -py /opt/project/code/pipelines/ingestion/raw_zone_consumer.py \
    --topic "ecommerce.${zone}" --raw-path "s3://${S3_BUCKET}/raw/${zone}/"
done

PYTHONPATH=code python -m pipelines.ingestion.kafka_producer --csv-path <csv_gzip_path> --speed 60
```

Airflow DAG는 `silver_events → silver_funnel → Gold 5개 → health_check` 순서로 실행한다.
