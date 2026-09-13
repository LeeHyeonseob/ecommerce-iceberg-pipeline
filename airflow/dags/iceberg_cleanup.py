"""Iceberg 삭제(cleanup) DAG.

expire_snapshots는 오래된 스냅샷을 만료시키고 그 스냅샷만 참조하던 파일을 삭제한다.
remove_orphan_files는 반대로 어떤 스냅샷도 참조하지 않는 파일을 삭제한다.

재작성과의 날짜 의존성은 없다. older_than이 보존 기간 이전이라 당일 재작성한 파일은
삭제 대상이 될 수 없다. 그래서 증분·재작성에 연결하지 않고 독립 cron으로 둔다.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from dag_utils import PIPELINE_DIR, parse_last_json

ALL_TABLES = [
    "glue.ecommerce_lakehouse.silver_events",
    "glue.ecommerce_lakehouse.silver_funnel",
    "glue.ecommerce_lakehouse.gold_daily_gmv",
    "glue.ecommerce_lakehouse.gold_funnel_daily",
    "glue.ecommerce_lakehouse.gold_category_gmv",
    "glue.ecommerce_lakehouse.gold_pipeline_sla",
    "glue.ecommerce_lakehouse.gold_data_quality",
]


@dag(
    dag_id="iceberg_cleanup",
    # 일요일 06:00 UTC. 증분(@daily, 00:00 UTC)과 시간을 벌려 풀 대기를 줄인다.
    schedule="0 6 * * 0",
    start_date=pendulum.datetime(2026, 9, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "tables": ",".join(ALL_TABLES),
        "retention_days": 30,
    },
    tags=["ecommerce", "iceberg", "maintenance", "cleanup"],
)
def iceberg_cleanup():
    # --as-of를 스케줄 시각으로 고정한다. 넘기지 않으면 재시도마다 now-30d가 다시
    # 계산돼 삭제 범위가 조금씩 넓어지고, 로그만으로 범위를 재현할 수 없다.
    @task.bash(
        pool="spark_pool",
        output_processor=parse_last_json,
        env={
            "MAINT_TABLES": "{{ params.tables }}",
            "MAINT_RETENTION_DAYS": "{{ params.retention_days }}",
            "MAINT_AS_OF": "{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}",
        },
        append_env=True,
    )
    def cleanup() -> str:
        return f"""
        set -euo pipefail
        docker exec spark-runner python {PIPELINE_DIR}/iceberg_maintenance.py \\
          --s3-bucket "$S3_BUCKET" \\
          --aws-region "${{AWS_REGION:-ap-northeast-2}}" \\
          --steps expire_snapshots,remove_orphan_files \\
          --tables "$MAINT_TABLES" \\
          --retention-days "$MAINT_RETENTION_DAYS" \\
          --as-of "$MAINT_AS_OF"
        """

    cleanup()


iceberg_cleanup()
