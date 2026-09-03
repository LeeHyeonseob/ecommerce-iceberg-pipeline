import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


PIPELINE_DIR = "/opt/project/code/pipelines"
def parse_last_json(output: str) -> dict:
    """명령 출력에서 마지막 JSON 결과만 XCom으로 반환한다."""
    import json

    for line in reversed(output.strip().splitlines()):
        if line.strip().startswith("{") and line.strip().endswith("}"):
            return json.loads(line.strip())
    raise RuntimeError("파이프라인이 JSON 결과를 반환하지 않았습니다")


@dag(
    dag_id="ecommerce_incremental",
    # 통합 테스트가 끝나고 품질 검사까지 붙인 뒤 운영 cron을 활성화한다.
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 24, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={"pipeline_env": "prod", "from_datetime": "", "to_datetime": ""},
    tags=["ecommerce", "iceberg", "incremental"],
)
def ecommerce_incremental():
    @task.bash(pool="spark_pool", output_processor=parse_last_json)
    def silver_events() -> str:
        return f"""
        set -e
        FROM_DATETIME='{{{{ params.from_datetime }}}}'
        TO_DATETIME='{{{{ params.to_datetime }}}}'
        if [ -n "$FROM_DATETIME" ] || [ -n "$TO_DATETIME" ]; then
          if [ -z "$FROM_DATETIME" ] || [ -z "$TO_DATETIME" ]; then
            echo 'from_datetime과 to_datetime은 함께 지정해야 합니다' >&2
            exit 1
          fi
        else
          FROM_DATETIME='{{{{ (data_interval_start - macros.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S") }}}}'
          TO_DATETIME='{{{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}}}'
        fi
        RUN_TOKEN=$(printf '%s' '{{{{ run_id }}}}' | sha256sum | cut -c1-16)
        BATCH_PATH="s3a://$S3_BUCKET/control/funnel-batches/run_id=$RUN_TOKEN/attempt={{{{ ti.try_number }}}}/"
        python {PIPELINE_DIR}/bronze_to_silver_events.py \\
          --env '{{{{ params.pipeline_env }}}}' \\
          --from-datetime "$FROM_DATETIME" \\
          --to-datetime "$TO_DATETIME" \\
          --batch-output-path "$BATCH_PATH"
        """

    @task.bash(pool="spark_pool", output_processor=parse_last_json)
    def silver_funnel() -> str:
        return f"""
        set -e
        if [ {{{{ ti.xcom_pull(task_ids='silver_events')['event_count'] }}}} -eq 0 ]; then
          echo '{{"affected_key_count": 0, "funnel_dates": []}}'
        else
          python {PIPELINE_DIR}/silver_events_to_funnel.py \\
            --mode incremental \\
            --env '{{{{ params.pipeline_env }}}}' \\
            --batch-input-path '{{{{ ti.xcom_pull(task_ids='silver_events')['batch_output_path'] }}}}'
        fi
        """

    @task.bash(pool="spark_pool", do_xcom_push=False)
    def gold() -> str:
        return f"""
        set -e
        EVENT_DATES='{{{{ ti.xcom_pull(task_ids='silver_events')['event_dates'] | join(',') }}}}'
        FUNNEL_DATES='{{{{ ti.xcom_pull(task_ids='silver_funnel')['funnel_dates'] | join(',') }}}}'
        if [ -z "$EVENT_DATES" ] && [ -z "$FUNNEL_DATES" ]; then
          echo '영향 날짜 없음 - Gold 건너뜀'
          exit 0
        fi
        ARGS="--env {{{{ params.pipeline_env }}}}"
        [ -z "$EVENT_DATES" ] || ARGS="$ARGS --event-dates $EVENT_DATES"
        [ -z "$FUNNEL_DATES" ] || ARGS="$ARGS --funnel-dates $FUNNEL_DATES"
        python {PIPELINE_DIR}/silver_to_gold.py $ARGS
        """

    events = silver_events()
    funnels = silver_funnel()
    events >> funnels
    gold_task = gold()
    events >> gold_task
    funnels >> gold_task


ecommerce_incremental()
