import argparse

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30_000)
    return parser.parse_args()


def build_table_env(checkpoint_interval_ms: int) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(checkpoint_interval_ms)
    return StreamTableEnvironment.create(env)


def create_source_table(t_env: StreamTableEnvironment, topic: str, bootstrap_servers: str) -> None:
    consumer_group = f"raw-zone-consumer-{topic.replace('.', '-')}"
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            event_time STRING,
            event_type STRING,
            product_id STRING,
            category_id STRING,
            category_code STRING,
            brand STRING,
            price STRING,
            user_id STRING,
            user_session STRING,
            event_id STRING,
            kafka_partition INT METADATA FROM 'partition' VIRTUAL,
            kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = '{consumer_group}',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'earliest'
        )
    """)


def create_sink_table(t_env: StreamTableEnvironment, raw_path: str) -> None:
    t_env.execute_sql(f"""
        CREATE TABLE raw_zone_sink (
            event_time STRING,
            event_type STRING,
            product_id STRING,
            category_id STRING,
            category_code STRING,
            brand STRING,
            price STRING,
            user_id STRING,
            user_session STRING,
            event_id STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP_LTZ(3),
            ingest_ts TIMESTAMP_LTZ(3),
            raw_date STRING,
            raw_hour STRING
        )
        PARTITIONED BY (raw_date, raw_hour)
        WITH (
            'connector' = 'filesystem',
            'path' = '{raw_path}',
            'format' = 'parquet',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '30 s'
        )
    """)


def run_insert(t_env: StreamTableEnvironment) -> None:
    # .wait()를 부르지 않음: 스트리밍 잡은 끝나지 않으므로 wait하면 flink run -d(detached)여도
    # 파이썬 드라이버가 블록돼서 CLI가 안 빠져나옴. 제출만 하고 바로 종료.
    t_env.execute_sql("""
        INSERT INTO raw_zone_sink
        SELECT
            event_time, event_type, product_id, category_id, category_code,
            brand, price, user_id, user_session, event_id,
            kafka_partition, kafka_offset, kafka_timestamp,
            CURRENT_TIMESTAMP AS ingest_ts,
            DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd') AS raw_date,
            DATE_FORMAT(CURRENT_TIMESTAMP, 'HH') AS raw_hour
        FROM kafka_source
    """)


def main() -> None:
    args = parse_args()
    t_env = build_table_env(args.checkpoint_interval_ms)
    zone_name = args.topic.rsplit(".", 1)[-1]
    t_env.get_config().set("pipeline.name", f"raw_zone_{zone_name}")
    create_source_table(t_env, args.topic, args.bootstrap_servers)
    create_sink_table(t_env, args.raw_path)
    run_insert(t_env)


if __name__ == "__main__":
    main()
