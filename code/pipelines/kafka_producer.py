import argparse
import csv
import gzip
import hashlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime

from dotenv import load_dotenv
from kafka import KafkaProducer

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

TOPIC_MAP = {
    "view": "ecommerce.view",
    "cart": "ecommerce.cart",
    "purchase": "ecommerce.purchase",
}

CSV_COLUMNS = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]


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
    raw = "|".join(row.get(col, "") for col in CSV_COLUMNS)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def parse_event_time(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S %Z")


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
    import json

    payload = {col: row.get(col) for col in CSV_COLUMNS}
    payload["event_id"] = event_id
    return json.dumps(payload, ensure_ascii=False)


def publish(producer: KafkaProducer, topic: str, event_id: str, row: dict) -> None:
    key = row.get("user_id")
    value = to_json(row, event_id)
    try:
        producer.send(topic, key=key, value=value)
    except Exception as e:
        logger.error(f"발행 실패 topic={topic} event_id={event_id}: {e}")


def main() -> None:
    args = parse_args()
    producer = build_producer(args.bootstrap_servers)

    prev_event_time: datetime | None = None
    sent = 0

    for row in read_events(args.csv_path, limit=args.limit):
        event_type = row["event_type"]
        topic = TOPIC_MAP.get(event_type)
        if topic is None:
            logger.warning(f"알 수 없는 event_type={event_type!r}, skip")
            continue

        curr_event_time = parse_event_time(row["event_time"])
        delay = compute_delay_seconds(prev_event_time, curr_event_time, args.speed)
        if delay > 0:
            time.sleep(delay)
        prev_event_time = curr_event_time

        event_id = make_event_id(row)
        publish(producer, topic, event_id, row)
        sent += 1

        if sent % 100_000 == 0:
            logger.info(f"sent={sent} last_event_time={row['event_time']}")

    producer.flush()
    logger.info(f"done. total_sent={sent}")


if __name__ == "__main__":
    main()
