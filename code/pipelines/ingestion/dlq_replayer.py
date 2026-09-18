import argparse
import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from pipelines.common.event_contract import (
    ALLOWED_TOPICS,
    DLQ_TOPIC,
    EVENT_TYPE_BY_TOPIC,
    compute_event_id,
)
from pipelines.common.event_validation import validate_event

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("dlq_replayer")

DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CONSUMER_TIMEOUT_MS = 5_000


class DlqRangeError(RuntimeError):
    """요청한 [from, to) 범위를 끝까지 읽었다고 보장할 수 없을 때 발생시킨다."""


@dataclass
class DlqRecord:
    """dlq_offset은 DLQ 토픽 자체의 offset이다. kafka_partition/kafka_offset은 dlq_sink 스키마
    그대로의 이름이지만 실제로는 원본(view/cart/purchase) 토픽의 좌표를 가리킨다 - 헷갈리지
    않도록 여기서는 original_ 접두사로만 다룬다."""

    dlq_offset: int
    original_payload: str
    original_topic: str
    original_kafka_partition: int
    original_kafka_offset: int
    reason_code: str
    failure_detail: str | None


@dataclass
class ReplayOutcome:
    dlq_offset: int
    original_topic: str
    original_kafka_partition: int
    original_kafka_offset: int
    reason_code: str
    status: str  # replayed | rejected | failed | dry_run
    detail: str | None = None
    original_event_id: str | None = None
    recomputed_event_id: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="DLQ에 격리된 이벤트를 재검증(+선택적 보정)해 원본 토픽으로 재발행한다."
    )
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--dlq-topic", default=DLQ_TOPIC)
    parser.add_argument("--dlq-partition", type=int, required=True)
    parser.add_argument("--from-offset", type=int, required=True, help="DLQ offset, 포함(inclusive)")
    parser.add_argument("--to-offset", type=int, required=True, help="DLQ offset, 미포함(exclusive)")
    parser.add_argument("--original-topic", required=True, choices=ALLOWED_TOPICS)
    parser.add_argument("--reason-code", default=None, help="지정하면 이 사유 코드인 레코드만 처리")
    parser.add_argument("--corrections", default=None, help="dlq_offset별 보정 필드가 담긴 JSONL 경로")
    parser.add_argument("--execute", action="store_true", help="생략하면 dry-run(재발행 없이 미리보기만)")
    parser.add_argument("--report-path", default=None)
    args = parser.parse_args()
    if args.to_offset <= args.from_offset:
        parser.error("--to-offset은 --from-offset보다 커야 합니다 ([from, to) 반열림 구간)")
    return args


def load_corrections(path: str | None, dlq_partition: int) -> dict[int, dict]:
    """보정 JSONL을 읽는다. 각 줄은 dlq_partition을 반드시 포함해야 하고, 이번 실행의
    --dlq-partition과 다르면 거부한다 - 파티션마다 offset이 독립적이라(파티션 0의 offset 10과
    파티션 1의 offset 10은 다른 레코드) 다른 파티션용 보정 파일을 잘못 재사용하는 실수를 막는다.
    같은 dlq_offset이 두 번 나오면 조용히 덮어쓰지 않고 오류로 처리한다."""
    if not path:
        return {}
    corrections: dict[int, dict] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no} JSON 파싱 실패: {exc}") from exc
            if "dlq_offset" not in obj:
                raise ValueError(f"{path}:{line_no}에 dlq_offset이 없습니다")
            if "dlq_partition" not in obj:
                raise ValueError(f"{path}:{line_no}에 dlq_partition이 없습니다")
            if int(obj["dlq_partition"]) != dlq_partition:
                raise ValueError(
                    f"{path}:{line_no}의 dlq_partition={obj['dlq_partition']}이 "
                    f"--dlq-partition={dlq_partition}과 다릅니다 - 다른 파티션용 보정 파일을 "
                    "잘못 넘겼을 수 있습니다"
                )
            offset = int(obj["dlq_offset"])
            if offset in corrections:
                raise ValueError(f"{path}:{line_no} dlq_offset={offset}이 중복됩니다")
            corrections[offset] = obj.get("corrections", {})
    return corrections


def fetch_dlq_records(
    bootstrap_servers: str,
    dlq_topic: str,
    dlq_partition: int,
    from_offset: int,
    to_offset: int,
) -> list[DlqRecord]:
    """[from_offset, to_offset) 범위를 전부 읽었다고 확인될 때만 결과를 반환한다.

    consumer_timeout_ms 안에 새 메시지가 없으면 순회가 조용히 끝나버리므로, 요청한 to_offset이
    실제로 존재하지 않는 범위(아직 안 왔거나 잘못 지정)라면 일부만 읽고 성공한 것처럼 끝날 수
    있다 - 그래서 시작 전에 beginning/end offset으로, 끝난 뒤에는 실제 소비 위치로 한 번 더
    검증한다. 완전히 못 읽었으면 재발행하지 않고 예외를 던진다.
    """
    consumer = KafkaConsumer(
        bootstrap_servers=[bootstrap_servers],
        value_deserializer=lambda v: v.decode("utf-8"),
        enable_auto_commit=False,
        consumer_timeout_ms=CONSUMER_TIMEOUT_MS,
    )
    tp = TopicPartition(dlq_topic, dlq_partition)
    try:
        consumer.assign([tp])

        beginning_offset = consumer.beginning_offsets([tp])[tp]
        if from_offset < beginning_offset:
            raise DlqRangeError(
                f"--from-offset={from_offset}가 이미 만료/삭제된 범위입니다 "
                f"(파티션 {dlq_partition}의 현재 시작 offset={beginning_offset})"
            )
        end_offset = consumer.end_offsets([tp])[tp]
        if to_offset > end_offset:
            raise DlqRangeError(
                f"--to-offset={to_offset}이 파티션 {dlq_partition}의 실제 끝 offset({end_offset})을 "
                "넘어섭니다 - 아직 도착하지 않았거나 범위를 잘못 지정했을 수 있습니다"
            )

        consumer.seek(tp, from_offset)
        records = []
        for msg in consumer:
            if msg.offset >= to_offset:
                break
            obj = json.loads(msg.value)
            records.append(
                DlqRecord(
                    dlq_offset=msg.offset,
                    original_payload=obj["original_payload"],
                    original_topic=obj["original_topic"],
                    original_kafka_partition=obj["kafka_partition"],
                    original_kafka_offset=obj["kafka_offset"],
                    reason_code=obj["reason_code"],
                    failure_detail=obj.get("failure_detail"),
                )
            )

        position = consumer.position(tp)
        if position < to_offset:
            raise DlqRangeError(
                f"[{from_offset}, {to_offset}) 범위를 끝까지 못 읽었습니다 "
                f"(consumer_timeout_ms={CONSUMER_TIMEOUT_MS}ms 안에 후속 메시지 없음, "
                f"실제 도달 위치={position})"
            )
    finally:
        consumer.close()
    return records


def build_candidate_fields(record: DlqRecord, correction: dict) -> dict | None:
    """원본 payload를 파싱해 보정 필드를 얹는다. 원문 자체가 JSON이 아니면 보정 불가로 None."""
    try:
        obj = json.loads(record.original_payload)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(obj, dict):
        return None
    obj.update(correction)
    return obj


def process_record(record: DlqRecord, correction: dict) -> tuple[ReplayOutcome, str | None, str | None]:
    """검증까지만 수행한다. 실제 발행 여부(dry-run/execute)는 호출자가 결정한다.

    순서가 중요하다: 먼저 검증해서 validate_event가 정규화한 값(문자열 강제 변환,
    dict/list -> None 등)을 얻고, 그 정규화된 값으로만 event_id를 계산·최종 payload를
    재구성한다. 검증 전 원시값을 그대로 해싱하면 예를 들어 category_code가 배열로 온
    경우 검증기는 이걸 None으로 취급하는데 event_id는 배열의 문자열 표현을 기준으로
    계산돼버려 "event_id = 실제 저장되는 내용의 해시"라는 계약이 깨진다.

    반환값: (outcome, 발행할 최종 payload json 또는 None, key로 쓸 user_id 또는 None)
    """
    base_outcome = dict(
        dlq_offset=record.dlq_offset,
        original_topic=record.original_topic,
        original_kafka_partition=record.original_kafka_partition,
        original_kafka_offset=record.original_kafka_offset,
        reason_code=record.reason_code,
    )

    fields = build_candidate_fields(record, correction)
    if fields is None:
        return (
            ReplayOutcome(**base_outcome, status="rejected", detail="원문이 JSON object가 아니라 보정할 수 없음"),
            None,
            None,
        )

    original_event_id = fields.get("event_id")
    expected_event_type = EVENT_TYPE_BY_TOPIC[record.original_topic]

    # event_id는 최종적으로 재계산해서 덮어쓸 것이므로, 1차 검증(정규화 목적)에서는
    # 원본 event_id를 임시로 그대로 쓴다 - "비어있지 않은 문자열인가"만 확인되면 된다.
    placeholder_json = json.dumps(fields, ensure_ascii=False)
    result = validate_event(placeholder_json, expected_event_type)
    if not result.is_valid:
        return (
            ReplayOutcome(
                **base_outcome,
                status="rejected",
                detail=f"{result.reason_code}: {result.failure_detail}",
                original_event_id=original_event_id,
            ),
            None,
            None,
        )

    normalized_fields = {
        "event_time": result.event_time,
        "event_type": result.event_type,
        "product_id": result.product_id,
        "category_id": result.category_id,
        "category_code": result.category_code,
        "brand": result.brand,
        "price": result.price,
        "user_id": result.user_id,
        "user_session": result.user_session,
    }
    recomputed_event_id = compute_event_id(normalized_fields)
    final_payload = {**normalized_fields, "event_id": recomputed_event_id}
    candidate_json = json.dumps(final_payload, ensure_ascii=False)

    return (
        ReplayOutcome(
            **base_outcome,
            status="valid",
            original_event_id=original_event_id,
            recomputed_event_id=recomputed_event_id,
        ),
        candidate_json,
        normalized_fields["user_id"],
    )


def replay(args: argparse.Namespace) -> dict:
    corrections = load_corrections(args.corrections, args.dlq_partition)
    records = fetch_dlq_records(
        args.bootstrap_servers, args.dlq_topic, args.dlq_partition, args.from_offset, args.to_offset
    )

    producer = None
    if args.execute:
        producer = KafkaProducer(
            bootstrap_servers=[args.bootstrap_servers],
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    outcomes: list[ReplayOutcome] = []
    counts = {"attempted": 0, "replayed": 0, "rejected": 0, "failed": 0, "skipped": 0}

    for record in records:
        if record.original_topic != args.original_topic:
            counts["skipped"] += 1
            continue
        if args.reason_code and record.reason_code != args.reason_code:
            counts["skipped"] += 1
            continue

        counts["attempted"] += 1
        correction = corrections.get(record.dlq_offset, {})
        outcome, candidate_json, key = process_record(record, correction)

        if outcome.status == "rejected":
            counts["rejected"] += 1
            outcomes.append(outcome)
            continue

        if not args.execute:
            outcome.status = "dry_run"
            outcomes.append(outcome)
            continue

        try:
            future = producer.send(args.original_topic, key=key, value=candidate_json)
            future.get(timeout=30)
        except Exception as exc:
            outcome.status = "failed"
            outcome.detail = str(exc)
            counts["failed"] += 1
            outcomes.append(outcome)
            continue

        outcome.status = "replayed"
        counts["replayed"] += 1
        outcomes.append(outcome)

    flush_error = None
    if producer is not None:
        try:
            producer.flush()
        except Exception as exc:
            # 개별 발행은 이미 future.get()으로 ACK를 확인했으므로 flush 실패가 성공 건수를
            # 뒤집지는 않는다 - 다만 리포트가 통째로 유실되지 않도록 예외를 삼키고 기록만 남긴다.
            flush_error = str(exc)
            logger.error(f"producer.flush() 실패: {exc}")
        finally:
            producer.close()

    report = {
        "meta": {
            "dlq_topic": args.dlq_topic,
            "dlq_partition": args.dlq_partition,
            "from_offset": args.from_offset,
            "to_offset": args.to_offset,
            "original_topic": args.original_topic,
            "reason_code": args.reason_code,
            "execute": args.execute,
            "flush_error": flush_error,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        },
        "counts": counts,
        "records": [asdict(o) for o in outcomes],
    }
    return report


def write_report(report: dict, report_path: str | None) -> str:
    if report_path is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        report_path = os.path.join("reports", f"dlq-replay-{timestamp}.json")
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return report_path


def main() -> None:
    args = parse_args()
    if not args.execute:
        logger.info("dry-run 모드입니다 - 실제 재발행 없이 미리보기만 합니다 (--execute로 실행)")
    report = replay(args)
    report_path = write_report(report, args.report_path)
    logger.info(f"리포트 저장: {report_path}")
    print(json.dumps(report["counts"], ensure_ascii=False))

    # rejected는 검증 규칙대로 걸러진 정상적인 결과라 실패가 아니다. failed(Kafka 발행 자체가
    # 안 된 것)만 종료 코드로 알린다 - 안 그러면 운영자나 자동화가 명령 성공만 보고 놓친다.
    if report["counts"]["failed"] > 0:
        raise RuntimeError(
            f"Kafka 재발행 실패 {report['counts']['failed']}건 - 리포트 확인: {report_path}"
        )


if __name__ == "__main__":
    main()
