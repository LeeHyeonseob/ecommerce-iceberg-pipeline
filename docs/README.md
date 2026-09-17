# 지식 저장소

이 디렉터리는 에이전트와 새 팀원이 설계의 이유, 이미 실패한 접근, 도메인 규칙을 재발견하지 않도록 보존한다. 사용자용 실행 설명과 실험 결과는 루트 [README.md](../README.md)가 기준이다.

## 문서 지도

| 종류 | 내용 | 먼저 읽을 때 |
| --- | --- | --- |
| [decisions/](decisions/) | 확정된 기술 결정과 대안 | 구조나 저장·갱신 방식을 바꿀 때 |
| [failures/](failures/) | 실제로 잘못된 결과를 낸 접근 | 집계·Spark/S3 설정을 수정할 때 |
| [domain/](domain/) | 용어, 데이터 계약, 워크플로우, 한계 | 스키마·KPI·퍼널을 바꿀 때 |
| [conventions/](conventions/) | 변경 규칙과 테스트 기준 | 구현과 리뷰 전후 |
| [operations/](operations/) | 실행, 진단, 장애 대응 | 인프라·Airflow·운영 작업 시 |

## 프로젝트 구조

```text
gzip CSV → Kafka → Flink → Bronze → Silver(events·funnel)
→ Gold(KPI) → Athena → Superset
```

GMV·카테고리 GMV·SLA는 `silver_events`, 전환 지표는 `silver_funnel`, Data Quality는 두 테이블을 입력으로 사용한다.

| 위치 | 책임 |
| --- | --- |
| `code/pipelines/event_contract.py` | Producer·Consumer가 공유하는 토픽·필드·시간 형식 계약 |
| `code/pipelines/event_validation.py` | PyFlink와 독립적인 입력 레코드 검증 |
| `code/pipelines/kafka_producer.py` | `event_id` 생성, event type별 토픽 라우팅 |
| `code/pipelines/raw_zone_consumer.py` | Kafka 원문을 검증해 정상은 Bronze, 실패는 DLQ로 분기 |
| `code/pipelines/bronze_to_silver_events.py` | 정제, dedup, Silver MERGE, 배치 산출물 |
| `code/pipelines/silver_events_to_funnel.py` | 세션 퍼널과 30일 cross-session 전환 계산 |
| `code/pipelines/silver_to_gold.py` | 이벤트·퍼널 grain별 KPI와 영향 날짜 재집계 |
| `airflow/dags/` | 증분 순서, 재시도, Spark 작업 직렬화, 재작성·삭제 유지보수 DAG |
| `code/ddl/` | Glue/Iceberg 테이블 계약 |
| `code/health-queries/` | 데이터 및 Iceberg metadata 진단 |
| `dashboard/superset/` | Athena 기반 대시보드 정의 |

Compose는 수집, 배치, BI 스택으로 분리한다. Spark는 UTC, Bronze `raw_datetime`은 수집 시각, Silver `event_date`는 이벤트 시각, `funnel_date`는 퍼널 시작 시각을 기준으로 한다.

## 기록 규칙

- 결정에는 배경, 선택, 이유, 포기한 대안, 결과와 번복 조건을 적는다.
- 실패에는 실제 증상, 원인, 현재 대안, 에이전트 지침을 적는다.
- 날짜를 확인할 수 없으면 임의로 만들지 않고 `날짜 미기록`으로 둔다.
- 계획이나 검토 사항을 현재 구현처럼 쓰지 않는다.
- 코드와 문서가 다르면 코드를 현재 동작의 근거로 삼고 문서를 함께 고친다.
