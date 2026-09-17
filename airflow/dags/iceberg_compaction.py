"""Iceberg 재작성(compaction) DAG. 삭제는 iceberg_cleanup으로 분리했다(docs/decisions/003).

schedule=None이다. ecommerce_incremental이 주간 조건을 판단해 trigger한다. Airflow 풀은
태스크 단위로 적용되므로 cron 시간차만으로는 증분과의 순서를 보장할 수 없다.

DAG param은 명령 문자열이 아니라 env로 전달한다. 문자열에 넣으면 Python의 허용 목록
검증보다 셸 해석이 먼저 일어난다.
"""
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task
from dag_utils import PIPELINE_DIR, parse_last_json, slack_alert_on_failure

# Gold는 COW overwrite로 파티션당 data file이 1개라 rewrite_data_files가 구조적으로 0건이다.
SILVER_TABLES = [
    "glue.ecommerce_lakehouse.silver_events",
    "glue.ecommerce_lakehouse.silver_funnel",
]
# health_check는 bare 테이블명을 받는다(--detail-tables). 비교 기준을 고정하기 위해
# params.tables를 좁혀도 상세 조회 대상은 Silver 2개를 유지한다.
HEALTH_DETAIL_TABLES = "silver_events,silver_funnel"
HEALTH_DETAIL_PARTITIONS = 31


def health_command(label: str) -> str:
    return f"""
    set -euo pipefail
    echo '--- health {label} ---'
    docker exec spark-runner python {PIPELINE_DIR}/operations/health_check.py \\
      --s3-bucket "$S3_BUCKET" \\
      --aws-region "${{AWS_REGION:-ap-northeast-2}}" \\
      --detail \\
      --detail-tables "{HEALTH_DETAIL_TABLES}" \\
      --detail-partitions {HEALTH_DETAIL_PARTITIONS}
    """


@dag(
    dag_id="iceberg_compaction",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 13, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": slack_alert_on_failure,
    },
    params={
        # 일회성 실측 때 한 테이블로 좁히거나 옵션을 주기 위해 노출한다.
        "tables": ",".join(SILVER_TABLES),
        "rewrite_options": "",          # 예: min-input-files=2
        "delete_rewrite_options": "",   # 예: min-input-files=2
    },
    tags=["ecommerce", "iceberg", "maintenance", "rewrite"],
)
def iceberg_compaction():
    @task.bash(pool="spark_pool", do_xcom_push=False)
    def health_before() -> str:
        return health_command("before")

    # 두 단계를 한 태스크에 묶는다. 쪼개면 그 사이에 dangling delete가 방치된다.
    @task.bash(
        pool="spark_pool",
        output_processor=parse_last_json,
        env={
            "MAINT_TABLES": "{{ params.tables }}",
            "MAINT_REWRITE_OPTS": "{{ params.rewrite_options }}",
            "MAINT_DELETE_OPTS": "{{ params.delete_rewrite_options }}",
        },
        append_env=True,
    )
    def rewrite() -> str:
        return f"""
        set -euo pipefail
        ARGS=(--steps rewrite_data_files,rewrite_position_delete_files --tables "$MAINT_TABLES")
        if [ -n "$MAINT_REWRITE_OPTS" ]; then
          IFS=',' read -ra PAIRS <<< "$MAINT_REWRITE_OPTS"
          for pair in "${{PAIRS[@]}}"; do ARGS+=(--rewrite-option "$pair"); done
        fi
        if [ -n "$MAINT_DELETE_OPTS" ]; then
          IFS=',' read -ra PAIRS <<< "$MAINT_DELETE_OPTS"
          for pair in "${{PAIRS[@]}}"; do ARGS+=(--delete-rewrite-option "$pair"); done
        fi
        docker exec spark-runner python {PIPELINE_DIR}/operations/iceberg_maintenance.py \\
          --s3-bucket "$S3_BUCKET" \\
          --aws-region "${{AWS_REGION:-ap-northeast-2}}" \\
          "${{ARGS[@]}}"
        """

    @task.bash(pool="spark_pool", do_xcom_push=False)
    def health_after() -> str:
        return health_command("after")

    health_before() >> rewrite() >> health_after()


iceberg_compaction()
