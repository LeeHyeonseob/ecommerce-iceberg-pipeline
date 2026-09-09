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
- `health_check.py`: Silver/Gold freshness·파일 상태와 Iceberg metadata
- Airflow JSON 파싱 실패: Spark 로그의 선행 예외와 마지막 출력 확인
- 빈 배치: `batch_output_path=null`, Funnel·Gold skip 확인
- S3 오류: bucket, 자격증명, region, S3/S3A 설정 확인
- Superset 수치 중복: `ALL`, category, `dim_type` 필터 확인

## Iceberg 유지보수

MOR Silver에 필요한 목표 순서는 다음과 같다.

```text
rewrite_position_delete_files → rewrite_data_files → rewrite_manifests
→ expire_snapshots(retain_last=1, 기본 30일)
→ remove_orphan_files
```

Silver는 MOR이므로 position delete 파일 정리가 필요하다. Gold는 날짜 파티션 overwrite 중심의 COW를 유지한다. 유지보수와 증분은 같은 `spark_pool` 1슬롯으로 직렬화한다. 현재 유지보수 코드는 `rewrite_data_files`부터 실행하며, position delete 정리와 정기 스케줄은 아직 미구현이다.
