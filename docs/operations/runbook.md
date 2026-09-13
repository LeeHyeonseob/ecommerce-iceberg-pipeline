# 운영 Runbook

## 실행

환경변수는 `.env.example`, 상세 명령은 루트 `README.md`를 따른다.

```text
Flink raw zone jobs 시작
→ Producer 실행
→ Airflow ecommerce_incremental 실행
→ health_check 로그와 Superset 운영 탭 확인
```

## 진단

- `verify_raw_zones.py`: Bronze 파일 크기와 도착 상태
- `health_check.py`: Silver/Gold freshness·파일 상태와 Iceberg metadata. `--detail`은 `detail/` 디렉터리의 파티션별 쿼리까지 실행하며, `--detail-tables`로 대상 테이블을, `--detail-partitions`로 테이블별 최근 파티션 수(기본 14)를 제한한다
- Airflow JSON 파싱 실패: Spark 로그의 선행 예외와 마지막 출력 확인
- 빈 배치: `batch_output_path=null`, Funnel·Gold skip 확인
- S3 오류: bucket, 자격증명, region, S3/S3A 설정 확인
- Superset 수치 중복: `ALL`, category, `dim_type` 필터 확인

## Iceberg 유지보수

재작성과 삭제는 성격이 달라 DAG를 나눴다. 재작성은 기존 파일을 즉시 물리 삭제하지 않고 새 snapshot을
생성하므로, snapshot 보존 기간 안에는 이전 상태를 조회할 수 있다. 삭제는 파일을 실제로 지우며,
cleanup이 지난 뒤에는 그 이전 상태로 돌아갈 수 없다. 재작성만으로는 저장 공간이 줄지 않고 오히려
늘어난다. 공간이 회수되는 것은 `expire_snapshots`가 스냅샷을 만료시킬 때다.

| DAG | 주기 | 단계 | 대상 |
| --- | --- | --- | --- |
| `iceberg_compaction` | 주 1회 (증분 `health_check` 성공 후 trigger) | `rewrite_data_files` → `rewrite_position_delete_files` | Silver 2개 |
| `iceberg_cleanup` | 주 1회 독립 cron (일 06:00 UTC) | `expire_snapshots` → `remove_orphan_files` | 7개 전부 |

- 순서는 입력 순서가 아니라 `iceberg_maintenance.py`의 `STEP_ORDER`로 고정된다. data file 재작성이
  dangling delete를 만들고 position delete 재작성이 그것을 정리하므로 뒤바꾸면 안 된다.
- Gold는 COW overwrite로 파티션당 data file이 1개라 `rewrite_data_files`가 구조적으로 0건이다.
  그래서 재작성 대상에서 제외한다.
- `rewrite_manifests`는 정기 실행하지 않는다. `commit.manifest-merge.enabled`(기본 true)와
  `commit.manifest.min-count-to-merge`(기본 100)로 쓰기 시 자동 병합되므로 무한 증가 위험이 없다.
  `iceberg/02_manifest_health.sql`로 개수를 관측하고, 필요하면
  `--steps rewrite_manifests`로 실행한다.
- 삭제는 `--as-of`로 기준 시각을 스케줄 시각에 고정한다. 넘기지 않으면 재시도마다 `now-30d`가
  다시 계산돼 범위가 넓어지고 로그만으로 범위를 재현할 수 없다.
- 증분과 재작성의 순서는 cron 시간차로 보장하지 않는다. Airflow 풀은 태스크 단위로 적용되므로
  1슬롯이 DAG 전체를 연속 구간으로 예약하지 않는다. 그래서 `TriggerDagRunOperator`로 연결한다.
- 주기는 확정값이 아니라 관측 후 조정할 초기값이다. 재작성 실행 시간과 실제 재작성량,
  파티션별 파일 수, Athena 조회 시간을 보고 정한다.

### 진단과 대응

- 그 주 증분이 실패하면 재작성도 트리거되지 않는다. 캐치업 경로가 없으므로 `iceberg_compaction`을
  수동 트리거한다.
- 테이블 하나가 실패해도 나머지는 계속 처리하고, 결과 JSON의 `failed_tables`에 기록된 뒤
  종료 코드 1로 끝난다. 실패한 테이블만 확인해 재실행한다.
- 삭제 단계 실패 시에는 재시도 전에 스냅샷·metadata 상태를 먼저 확인한다.

### 첫 실측 (기본 옵션)

`iceberg_compaction`을 `silver_funnel`만 대상으로, **옵션 없이** 수동 트리거한다.

```text
tables                  glue.ecommerce_lakehouse.silver_funnel
rewrite_options         (비움)
delete_rewrite_options  (비움)
```

DAG가 `health_before → rewrite → health_after` 순서로 돌므로 전후 상태가 같은 DAG run의 `health_before`와 `health_after` task 로그에 남는다.
기록할 것은 procedure 반환값(`rewritten_data_files_count`, `added_data_files_count`,
`rewritten_bytes_count`, `rewritten_delete_files_count`), `duration_sec`, 전후 파티션별 파일 수다.

반환값이 0이어도 고장이 아니다. 기본 임계값(`min-input-files=5`, `min-file-size`=target의 75%,
그룹 총량 > target) 안에 있다는 뜻이다. 0이 나온 뒤에 파티션별 분포를 보고 옵션 조정을 검토한다.

### 옵션 조정 (실측 이후에만)

현재 `silver_funnel`의 data file 83개·position delete 52개는 정상 운영 결과가 아니라 동일 배치
2회 재처리 테스트의 잔재다(docs/failures/003). 기본 옵션 실측에서 재작성이 0건이고 파티션별
파일 수가 2~4개에 머무는 것이 확인되면 아래 순서로 조정한다. **한 번에 하나씩만 바꾼다.**

1. `rewrite_options=min-input-files=2`만 적용해 실행한다. `delete_rewrite_options`는 비워 둔다.
2. data file이 파티션당 1개로 줄었는지, 재작성 바이트와 소요 시간이 얼마인지 확인한다.
3. 그 뒤에도 position delete file이 남아 있고 기본 옵션의 delete 재작성이 0건이었다면,
   그때 `delete_rewrite_options=min-input-files=2`를 적용해 다시 실행한다.

2단계를 건너뛰고 두 옵션을 동시에 바꾸면 어느 쪽이 무엇을 바꿨는지 귀속할 수 없다.
`rewrite_data_files`는 delete를 적용해 새 data file을 만들면서 옛 delete를 dangling으로 만들므로,
1단계만으로 delete file 상태가 달라질 수 있다. 그 변화를 먼저 본 뒤 3단계를 판단한다.

이는 잔재 정리용 일회성 설정이며 상시 정책이 아니다. `delete-file-threshold=1`의 상시 적용은
MOR의 쓰기 절감 효과를 잃으므로 쓰지 않는다.

파티션을 넘어 병합하지 않으므로 기대 결과는 평균 크기가 128MB에 가까워지는 것이 아니라
26개 다중 파일 파티션이 각 1개로 줄어 총 31개 부근이 되는 것이다. `docker exec` 직접 실행은
`spark_pool`을 우회해 증분과 겹칠 수 있으므로 쓰지 않는다.
