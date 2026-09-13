import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import dag, task
from dag_utils import PIPELINE_DIR, parse_last_json

HEALTH_SCRIPT = f"{PIPELINE_DIR}/health_check.py"

# Iceberg 재작성 주기. 0=월 ... 6=일. 부모 배치의 UTC 데이터 구간 기준으로 판단한다.
COMPACTION_WEEKDAY = 5


@dag(
    dag_id="ecommerce_incremental",
    schedule="@daily",
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
        {{% if params.from_datetime or params.to_datetime %}}
          if [ -z "$FROM_DATETIME" ] || [ -z "$TO_DATETIME" ]; then
            echo 'from_datetime과 to_datetime은 함께 지정해야 합니다' >&2
            exit 1
          fi
        {{% elif data_interval_start is defined and data_interval_end is defined %}}
          FROM_DATETIME='{{{{ (data_interval_start - macros.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S") }}}}'
          TO_DATETIME='{{{{ data_interval_end.strftime("%Y-%m-%d %H:%M:%S") }}}}'
        {{% else %}}
          FROM_DATETIME='{{{{ (dag_run.run_after - macros.timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S") }}}}'
          TO_DATETIME='{{{{ dag_run.run_after.strftime("%Y-%m-%d %H:%M:%S") }}}}'
        {{% endif %}}
        RUN_TOKEN=$(printf '%s' '{{{{ run_id }}}}' | sha256sum | cut -c1-16)
        BATCH_PATH="s3a://$S3_BUCKET/control/funnel-batches/run_id=$RUN_TOKEN/attempt={{{{ ti.try_number }}}}/"
        docker exec spark-runner python {PIPELINE_DIR}/bronze_to_silver_events.py \\
          --env '{{{{ params.pipeline_env }}}}' \\
          --from-datetime "$FROM_DATETIME" \\
          --to-datetime "$TO_DATETIME" \\
          --batch-output-path "$BATCH_PATH"
        """

    @task.bash(
        pool="spark_pool",
        output_processor=parse_last_json,
        env={
            "EVENT_COUNT": "{{ ti.xcom_pull(task_ids='silver_events')['event_count'] }}",
            "BATCH_PATH": "{{ ti.xcom_pull(task_ids='silver_events')['batch_output_path'] }}",
        },
        append_env=True,
    )
    def silver_funnel() -> str:
        return f"""
        set -e
        if [ "$EVENT_COUNT" -eq 0 ]; then
          echo '{{"affected_key_count": 0, "funnel_dates": []}}'
        else
          docker exec spark-runner python {PIPELINE_DIR}/silver_events_to_funnel.py \\
            --mode incremental \\
            --env '{{{{ params.pipeline_env }}}}' \\
            --batch-input-path "$BATCH_PATH"
        fi
        """

    @task.bash(
        pool="spark_pool",
        do_xcom_push=False,
        env={
            "EVENT_DATES": "{{ ti.xcom_pull(task_ids='silver_events')['event_dates'] | join(',') }}",
            "FUNNEL_DATES": "{{ ti.xcom_pull(task_ids='silver_funnel')['funnel_dates'] | join(',') }}",
        },
        append_env=True,
    )
    def gold() -> str:
        return f"""
        set -e
        if [ -z "$EVENT_DATES" ] && [ -z "$FUNNEL_DATES" ]; then
          echo '영향 날짜 없음 - Gold 건너뜀'
          exit 0
        fi
        ARGS="--env {{{{ params.pipeline_env }}}}"
        [ -z "$EVENT_DATES" ] || ARGS="$ARGS --event-dates $EVENT_DATES"
        [ -z "$FUNNEL_DATES" ] || ARGS="$ARGS --funnel-dates $FUNNEL_DATES"
        docker exec spark-runner python {PIPELINE_DIR}/silver_to_gold.py $ARGS
        """

    @task.bash(pool="spark_pool", do_xcom_push=False)
    def health_check() -> str:
        return f"""
        set -e
        docker exec spark-runner python {HEALTH_SCRIPT} \\
          --s3-bucket "$S3_BUCKET" \\
          --aws-region "${{AWS_REGION:-ap-northeast-2}}"
        """

    # 재작성은 주 1회다. 요일 판단을 증분 쪽에 두어, 소비 DAG가 조건을 모르게 한다.
    @task.short_circuit
    def is_compaction_day(data_interval_end=None) -> bool:
        return data_interval_end.weekday() == COMPACTION_WEEKDAY

    # 재작성 완료를 기다리지 않는다. 기다리면 증분 완료 시각이 유지보수 실패에 종속된다.
    # retries=0으로 둬서 trigger 재시도가 중복 실행을 만들지 않게 한다.
    trigger_compaction = TriggerDagRunOperator(
        task_id="trigger_compaction",
        trigger_dag_id="iceberg_compaction",
        wait_for_completion=False,
        retries=0,
        pool="default_pool",
    )

    events = silver_events()
    funnels = silver_funnel()
    events >> funnels
    gold_task = gold()
    health = health_check()
    events >> gold_task
    funnels >> gold_task
    gold_task >> health
    health >> is_compaction_day() >> trigger_compaction


ecommerce_incremental()
