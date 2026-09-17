# 개발과 검증 규칙

## 변경 영향 지도

| 변경 | 함께 확인할 위치 |
| --- | --- |
| 원본 컬럼·event ID | Producer 컬럼, Flink schema, Bronze→Silver transform, Silver DDL |
| Silver Events 컬럼 | DDL, `SILVER_COLUMNS`, 배치 산출물, Funnel·Gold·Superset |
| Funnel 컬럼·의미 | DDL, `FUNNEL_COLUMNS`, full/incremental 경로, Gold·Superset |
| Gold 컬럼 | DDL, `GOLD_COLUMNS`, builder SQL, Superset export |
| cross-session window | 후보·evidence 파티션, 도메인 규칙, snapshot 복구 범위와의 운영 관계 |
| Airflow 출력 | Spark 마지막 JSON, output processor, XCom, 빈 배치 분기 |
| Spark/Iceberg 버전 | requirements, 공통 Spark 설정, Docker JRE/Hadoop 호환성 |

## 구현 규칙

- 데이터 grain과 논리 키를 먼저 확인한다.
- 지연 도착이 과거 event/funnel 날짜를 바꾸는지 확인한다.
- MERGE의 영향 파티션 조건을 유지한다.
- 0행이 된 Gold 날짜의 기존 데이터도 제거한다.
- Spark 작업의 마지막 JSON 계약을 유지한다.
- 테이블 쓰기 전 컬럼 순서와 타입을 DDL과 비교한다.
- 대량 데이터는 XCom에 넣지 않는다.
- Iceberg 쓰기와 유지보수는 `spark_pool` 밖에서 동시에 실행하지 않는다. `docker exec spark-runner`로 직접 실행하면 풀 슬롯을 획득하지 않아 증분과 겹칠 수 있다. 유지보수 실행과 실측은 Airflow DAG를 통한다.

## 최소 검증

Python 변경 시 문법 검사를 수행한다.

```bash
python -m compileall code/pipelines airflow/dags
```

DAG를 변경했다면 Airflow 실행 환경에서 import 오류도 확인한다. `compileall`은 Python 문법만 검사하므로 DAG 로딩 성공을 보장하지 않는다.

```bash
docker compose -f infra/docker-compose.airflow.yml exec \
  airflow-scheduler airflow dags list-import-errors
```

Docker 또는 Airflow 환경을 사용할 수 없어 DAG import 검증을 실행하지 못했다면 작업 결과에 명시한다.

## 통합 검증

- 작은 `--limit`으로 Producer와 세 raw zone을 확인한다.
- 같은 수집 구간을 두 번 실행해 Silver event count가 변하지 않는지 확인한다.
- full과 incremental Funnel/Gold의 양방향 차집합이 `updated_at` 제외 0건인지 확인한다.
- 늦은 view/cart와 더 이른 cross-session purchase를 검증한다.
- Gold 기간 합계와 비율을 분자·분모 합계로 다시 계산한다.
- health query와 Superset 차트가 로드되는지 확인한다.

## 리뷰 위험 신호

- Funnel에서 GMV를 계산함
- purchase event count를 주문 건수라고 표현함
- event 날짜만 갱신하고 과거 funnel 날짜를 누락함
- `converted_later=0`인 행만 재검사함
- 일별 비율을 기간 평균으로 사용함
- `ALL`과 세부 category 또는 여러 `dim_type`을 함께 합산함

입력 계약은 `tests/test_event_validation.py`에서 단위 테스트한다. 전체 파이프라인을
자동 재현하는 통합 테스트 suite는 아직 없으므로, AWS 의존 검증을 실행하지 못했으면
결과에 명시한다.
