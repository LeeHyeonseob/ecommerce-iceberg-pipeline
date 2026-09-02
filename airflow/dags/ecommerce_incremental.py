import hashlib
import json
import os
import subprocess
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task


PIPELINE_DIR = "/opt/project/code/pipelines"
OVERLAP = timedelta(hours=2)


def run_pipeline(command: list[str]) -> dict:
    """Spark 로그를 출력하고 마지막 JSON을 XCom으로 반환한다."""
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    result = None
    assert process.stdout is not None
    for line in process.stdout:
        print(line, end="")
        stripped = line.strip()
        if stripped.startswith("{") and stripped.endswith("}"):
            try:
                result = json.loads(stripped)
            except json.JSONDecodeError:
                pass
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)
    if result is None:
        raise RuntimeError("파이프라인이 JSON 결과를 반환하지 않았습니다")
    return result


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
    @task(pool="spark_pool")
    def silver_events() -> dict:
        context = get_current_context()
        params = context["params"]
        pipeline_env = params["pipeline_env"]
        if bool(params["from_datetime"]) != bool(params["to_datetime"]):
            raise ValueError("from_datetime과 to_datetime은 함께 지정해야 합니다")
        if params["from_datetime"]:
            interval_start = pendulum.parse(params["from_datetime"], tz="UTC")
            interval_end = pendulum.parse(params["to_datetime"], tz="UTC")
        else:
            interval_start = context["data_interval_start"] - OVERLAP
            interval_end = context["data_interval_end"]
        run_token = hashlib.sha256(context["run_id"].encode()).hexdigest()[:16]
        try_number = context["task_instance"].try_number
        batch_path = (
            f"s3a://{os.environ['S3_BUCKET']}/control/funnel-batches/"
            f"run_id={run_token}/attempt={try_number}/"
        )
        return run_pipeline([
            "python",
            f"{PIPELINE_DIR}/bronze_to_silver_events.py",
            "--env",
            pipeline_env,
            "--from-datetime",
            interval_start.format("YYYY-MM-DD HH:mm:ss"),
            "--to-datetime",
            interval_end.format("YYYY-MM-DD HH:mm:ss"),
            "--batch-output-path",
            batch_path,
        ])

    @task(pool="spark_pool")
    def silver_funnel(events_result: dict) -> dict:
        pipeline_env = get_current_context()["params"]["pipeline_env"]
        if events_result["event_count"] == 0:
            return {"affected_key_count": 0, "funnel_dates": []}
        return run_pipeline([
            "python",
            f"{PIPELINE_DIR}/silver_events_to_funnel.py",
            "--mode",
            "incremental",
            "--env",
            pipeline_env,
            "--batch-input-path",
            events_result["batch_output_path"],
        ])

    @task(pool="spark_pool")
    def gold(events_result: dict, funnel_result: dict) -> dict:
        pipeline_env = get_current_context()["params"]["pipeline_env"]
        event_dates = events_result["event_dates"]
        funnel_dates = funnel_result["funnel_dates"]
        if not event_dates and not funnel_dates:
            return {"skipped": True, "reason": "영향 날짜 없음"}

        command = [
            "python",
            f"{PIPELINE_DIR}/silver_to_gold.py",
            "--env",
            pipeline_env,
        ]
        if event_dates:
            command.extend(["--event-dates", ",".join(event_dates)])
        if funnel_dates:
            command.extend(["--funnel-dates", ",".join(funnel_dates)])
        subprocess.run(command, check=True)
        return {"event_dates": event_dates, "funnel_dates": funnel_dates}

    events = silver_events()
    funnels = silver_funnel(events)
    gold(events, funnels)


ecommerce_incremental()
