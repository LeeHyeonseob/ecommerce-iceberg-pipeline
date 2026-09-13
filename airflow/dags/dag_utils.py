"""DAG 공용 유틸. 세 DAG에 복제돼 있던 출력 파서를 하나로 합친 것."""
import json

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
