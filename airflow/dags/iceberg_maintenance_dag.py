from datetime import timedelta

import pendulum
from airflow.sdk import dag, task


PIPELINE_DIR = "/opt/project/code/pipelines"


def parse_last_json(output: str) -> dict:
    import json

    for line in reversed(output.strip().splitlines()):
        if line.strip().startswith("{") and line.strip().endswith("}"):
            return json.loads(line.strip())
    raise RuntimeError("유지보수 스크립트가 JSON 결과를 반환하지 않았습니다")


@dag(
    dag_id="iceberg_maintenance",
    schedule=None,
    start_date=pendulum.datetime(2026, 9, 3, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ecommerce", "iceberg", "maintenance"],
)
def iceberg_maintenance():
    @task.bash(pool="spark_pool", output_processor=parse_last_json)
    def maintain() -> str:
        return f"""
        docker exec spark-runner python {PIPELINE_DIR}/iceberg_maintenance.py \\
          --s3-bucket "$S3_BUCKET" \\
          --aws-region "${{AWS_REGION:-ap-northeast-2}}"
        """

    maintain()


iceberg_maintenance()
