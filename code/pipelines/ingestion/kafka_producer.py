import argparse
import csv
import gzip
import hashlib
import json
import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

from pipelines.common.event_contract import EVENT_FIELDS, EVENT_TIME_FORMAT, TOPIC_BY_EVENT_TYPE

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kafka_producer.log"),
    ],
)
logger = logging.getLogger("kafka_producer")

DEFAULT_CSV_PATH = os.environ.get("CSV_PATH")
DEFAULT_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

@dataclass
class ProducerArgs:
    csv_path: str
    bootstrap_servers: str
    speed: float
    limit: int | None


def parse_args() -> ProducerArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv-path", default=DEFAULT_CSV_PATH, required=DEFAULT_CSV_PATH is None)
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS)
    parser.add_argument("--speed", type=float, default=60.0)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    return ProducerArgs(
        csv_path=args.csv_path,
        bootstrap_servers=args.bootstrap_servers,
        speed=args.speed,
        limit=args.limit,
    )


def read_events(csv_path: str, limit: int | None = None):
    with gzip.open(csv_path, mode="rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, fieldnames=None)
        for i, row in enumerate(reader):
            if limit is not None and i >= limit:
                break
            yield row


def make_event_id(row: dict) -> str:
    raw = "|".join(row.get(col, "") for col in EVENT_FIELDS)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_event_time(value: str) -> datetime:
    return datetime.strptime(value, EVENT_TIME_FORMAT)


def compute_delay_seconds(prev_event_time: datetime | None, curr_event_time: datetime, speed: float) -> float:
    if prev_event_time is None:
        return 0.0
    real_gap = (curr_event_time - prev_event_time).total_seconds()
    return max(0.0, real_gap / speed)


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_servers],
            value_serializer=lambda v: v.encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )
        logger.info(f"Kafka producer 연결 성공: {bootstrap_servers}")
        return producer
    except Exception as e:
        logger.error(f"Kafka producer 연결 실패: {e}")
        raise


def to_json(row: dict, event_id: str) -> str:
    payload = {col: row.get(col) for col in EVENT_FIELDS}
    payload["event_id"] = event_id
    return json.dumps(payload, ensure_ascii=False)


class SendCounter:
    """send()는 비동기라 콜백으로만 실제 성공/실패를 알 수 있다."""

    MAX_LOGGED_FAILURES = 20

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.confirmed = 0
        self.failed = 0

    def on_success(self, _metadata) -> None:
        with self._lock:
            self.confirmed += 1

    def _register_failure(self) -> int:
        with self._lock:
            self.failed += 1
            return self.failed

    def _log_failure(self, failed_count: int, topic: str, event_id: str, exc: Exception) -> None:
        if failed_count <= self.MAX_LOGGED_FAILURES:
            logger.error(f"발행 실패 topic={topic} event_id={event_id}: {exc}")
        elif failed_count == self.MAX_LOGGED_FAILURES + 1:
            logger.error(f"발행 실패가 {self.MAX_LOGGED_FAILURES}건을 넘어 이후는 집계만 함")

    def record_immediate_failure(self, topic: str, event_id: str, exc: Exception) -> None:
        self._log_failure(self._register_failure(), topic, event_id, exc)

    def make_errback(self, topic: str, event_id: str):
        def _on_error(exc: Exception) -> None:
            self._log_failure(self._register_failure(), topic, event_id, exc)

        return _on_error


def publish(producer: KafkaProducer, topic: str, event_id: str, row: dict, counter: SendCounter) -> None:
    key = row.get("user_id")
    value = to_json(row, event_id)
    try:
        future = producer.send(topic, key=key, value=value)
    except Exception as e:
        counter.record_immediate_failure(topic, event_id, e)
        return
    future.add_callback(counter.on_success)
    future.add_errback(counter.make_errback(topic, event_id))


def main() -> None:
    args = parse_args()
    producer = build_producer(args.bootstrap_servers)

    prev_event_time: datetime | None = None
    counter = SendCounter()
    attempted = 0

    for row in read_events(args.csv_path, limit=args.limit):
        event_type = row["event_type"]
        topic = TOPIC_BY_EVENT_TYPE.get(event_type)
        if topic is None:
            logger.warning(f"알 수 없는 event_type={event_type!r}, skip")
            continue

        curr_event_time = parse_event_time(row["event_time"])
        delay = compute_delay_seconds(prev_event_time, curr_event_time, args.speed)
        if delay > 0:
            time.sleep(delay)
        prev_event_time = curr_event_time

        event_id = make_event_id(row)
        publish(producer, topic, event_id, row, counter)
        attempted += 1

        if attempted % 100_000 == 0:
            logger.info(f"attempted={attempted} last_event_time={row['event_time']}")

    flush_error: Exception | None = None
    try:
        producer.flush()
    except Exception as e:
        flush_error = e
        logger.error(f"producer.flush() 실패: {e}")

    accounted = counter.confirmed + counter.failed
    if accounted != attempted:
        logger.error(
            f"집계 불일치: attempted={attempted}인데 confirmed+failed={accounted} "
            "(콜백이 아직 안 왔거나 유실됐을 수 있음)"
        )

    logger.info(
        f"done. attempted={attempted} confirmed={counter.confirmed} failed={counter.failed}"
    )

    if counter.failed > 0 or flush_error is not None or accounted != attempted:
        raise RuntimeError(
            f"Kafka 발행 실패: attempted={attempted} confirmed={counter.confirmed} "
            f"failed={counter.failed}" + (f", flush_error={flush_error}" if flush_error else "")
        )


if __name__ == "__main__":
    main()
