"""DAG 공용 유틸. 세 DAG에 복제돼 있던 출력 파서를 하나로 합친 것."""
import json
import os

import requests

PIPELINE_DIR = "/opt/project/code/pipelines"


def parse_last_json(output: str) -> dict:
    """명령 출력의 마지막 줄을 JSON으로 파싱한다.

    역방향으로 JSON처럼 보이는 줄을 찾으면, 최종 JSON을 내기 전에 죽은 실행에서
    앞선 테이블의 중간 출력을 집어 성공으로 오인한다. 그래서 마지막 비어 있지 않은
    줄만 검증한다. Spark 작업은 마지막 줄에 항상 JSON을 출력한다.
    """
    lines = [line.strip() for line in output.strip().splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("작업이 아무 출력도 남기지 않았습니다")
    last = lines[-1]
    try:
        return json.loads(last)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"마지막 출력 줄이 JSON이 아닙니다: {last[:300]}") from exc


def slack_alert_on_failure(context: dict) -> None:
    """재시도를 모두 소진하고 최종 FAILED가 된 태스크만 Slack으로 알린다.

    on_failure_callback은 state==FAILED일 때만 호출된다(UP_FOR_RETRY는 on_retry_callback).
    알림 전송 자체가 실패해도 태스크의 실패 처리를 방해하면 안 되므로 예외를 삼킨다.
    """
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    ti = context["ti"]
    error = str(context.get("exception") or "원인 정보 없음")
    error_summary = error.splitlines()[0][:500]
    text = (
        f"[CRITICAL][FIRING] Airflow 태스크 실패\n"
        f"{ti.dag_id}.{ti.task_id} (run_id={context['run_id']}, try={ti.try_number})\n"
        f"{error_summary}\n"
        f"{ti.log_url}"
    )
    # Grafana Slack 알림과 같은 색상바를 내려면 plain text가 아니라 attachment로 보내야 한다.
    # "danger"는 Slack이 인식하는 사전 정의 색상명(빨강)이다.
    payload = {"attachments": [{"color": "danger", "text": text}]}
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as exc:
        print(f"Slack 알림 전송 실패: {exc}")
