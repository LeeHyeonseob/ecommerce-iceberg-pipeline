# 003. 동일 배치 재처리 시 MOR 파일 증가

## 시도한 날짜

2026-09-09

## 시도한 이유

Silver를 COW에서 MOR로 전환한 뒤 같은 입력을 반복 처리해 멱등성과 파일 변화를 확인했다.

## 실패 증상

- 결과 행 수와 KPI는 같고 논리 키 중복도 0건이었다.
- 그런데 같은 Funnel 배치를 두 번 처리하자 `silver_funnel`의 data file은 31개에서 83개, position delete file은 0개에서 52개로 증가했다.
- `silver_events`도 동일 구간 재처리 후 data file 2개와 position delete file 2개가 추가됐다.

즉 결과 관점의 멱등성은 지켰지만, 동일 입력이 새 파일을 만들지 않는 물리적 멱등성은 지키지 못했다.

### 실측 결과

검증 구간은 `[2026-08-24 02:53:00, 02:54:00) UTC`다. 22,253개 event와 14,865개 funnel key를 처리했으며, Apple M2 16GB에서 Spark driver 3GB로 실행했다.

| 테이블 | 시점 | data files | 평균 data file | position delete files | delete bytes |
| --- | --- | ---: | ---: | ---: | ---: |
| `silver_events` | 전환 전 | 34 | 103,478,823 bytes | 0 | 0 |
| `silver_events` | 동일 구간 MERGE 후 | 36 | 97,808,588 bytes | 2 | 77,491 |
| `silver_funnel` | 전환 전 | 31 | 30,133,216 bytes | 0 | 0 |
| `silver_funnel` | 동일 배치 2회 MERGE 후 | 83 | 11,267,458 bytes | 52 | 148,832 |

- 반복 실행 전후 Funnel 전체 행 수: 27,785,942
- 두 Silver 테이블의 논리 키 중복: 0
- Silver Funnel과 Gold `ALL` 합계 일치: views 27,783,702, carts 628,209, purchases 690,372, purchases_later 394,493
- 두 Funnel 실행 모두 영향 키 14,865개, 지연 전환 210개로 동일

## 실패 원인

- Silver 변환 과정에서 실행할 때마다 `updated_at = current_timestamp()`를 생성한다.
- MERGE가 `WHEN MATCHED THEN UPDATE SET *`라 실제 데이터가 같아도 매칭된 모든 행을 갱신한다.
- MOR은 갱신된 기존 행을 position delete file에 표시하고 새 행을 data file에 기록하므로 불필요한 UPDATE가 파일 증가로 이어졌다.

## 현재 대응

해결 완료 (2026-09-10).

`updated_at`을 제외한 실제 데이터 컬럼이 달라졌을 때만 UPDATE하도록 두 Silver MERGE에 변경 감지 조건을 추가했다. 비교는 `NOT (t.col <=> s.col)`의 OR 결합이라 NULL 컬럼도 안전하게 판정한다.

```sql
WHEN MATCHED AND (NOT (t.`col` <=> s.`col`) OR ...) THEN UPDATE SET *
```

### 수정 후 재검증

동일 배치 재실행 시 data/delete file 증가는 방지했으며, 변경 파티션이 없는 빈 snapshot은 1개 생성됐다.

| 테이블 | data files | position delete files | snapshots |
| --- | ---: | ---: | ---: |
| `silver_events` | 36 → 36 | 2 → 2 | 5 → 6 |
| `silver_funnel` | 83 → 83 | 52 → 52 | 4 → 5 |

MERGE 자체는 매칭 행이 없어도 commit되므로 snapshot 1개는 계속 늘어난다. 파일 증가라는 원인은 제거했지만 snapshot 증가까지 없앤 것은 아니다.

빈 snapshot은 `expire_snapshots`가 보존 기간 이후 정리한다. 기존에 쌓인 파일은 compaction 대상이며, compaction은 이미 만들어진 파일을 정리할 뿐 불필요한 UPDATE의 원인을 없애지 못하므로 변경 감지를 먼저 적용한 이 순서가 맞다.

## 에이전트 지침

Silver MERGE를 수정할 때 무조건적인 matched UPDATE를 다시 도입하지 않는다. 재처리 검증에서는 중복과 KPI뿐 아니라 snapshot 및 data/delete file 변화도 확인한다.

## 구현·검증 근거

- [Silver Events MERGE](../../code/pipelines/bronze_to_silver_events.py)
- [Silver Funnel MERGE](../../code/pipelines/silver_events_to_funnel.py)
