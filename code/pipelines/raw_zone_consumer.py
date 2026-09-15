import argparse
from decimal import Decimal, InvalidOperation

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment
from pyflink.table.udf import ScalarFunction, udf


class BusinessMetricFunction(ScalarFunction):
    """이벤트를 통과시키며 저카디널리티 실시간 지표만 부수효과로 남긴다.

    topic 1개당 job 1개(view/cart/purchase)라 event_type을 label로 쓰지
    않아도 job_name으로 이미 구분된다. purchase 금액 누적은 event_type을 직접
    검사해서 판단한다(topic 소속만으로 판단하면 잘못 유입된 이벤트까지 셀 수 있음).
    원본에 통화가 없어 금액 단위를 가정하지 않는다 - PyFlink Counter는 정수만
    받으므로 100배 정수로 스케일링하고("cents"처럼 통화를 암시하는 이름은 쓰지 않음),
    Decimal로 변환해 float 반올림 오차를 피한다.
    """

    PURCHASE_EVENT_TYPE = "purchase"

    def __init__(self, is_purchase_topic: bool):
        self.is_purchase_topic = is_purchase_topic

    def open(self, function_context):
        metrics = function_context.get_metric_group().add_group("business")
        self.event_counter = metrics.counter("event_count")
        if self.is_purchase_topic:
            self.purchase_amount_x100 = metrics.counter("purchase_amount_x100")

    def eval(self, event_type, price):
        self.event_counter.inc()
        if (
            self.is_purchase_topic
            and event_type == self.PURCHASE_EVENT_TYPE
            and price is not None
        ):
            try:
                amount = Decimal(price)
                if amount.is_finite() and amount >= 0:
                    self.purchase_amount_x100.inc(int(amount * 100))
            except InvalidOperation:
                pass
        return event_type


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
            raw_datetime STRING
        )
        PARTITIONED BY (raw_datetime)
        WITH (
            'connector' = 'filesystem',
            'path' = '{raw_path}',
            'format' = 'parquet',
            'sink.partition-commit.policy.kind' = 'success-file',
            'sink.rolling-policy.rollover-interval' = '1 min',
            'sink.rolling-policy.check-interval' = '30 s'
        )
    """)


def register_business_metric_udf(t_env: StreamTableEnvironment, zone_name: str) -> None:
    is_purchase_topic = zone_name == "purchase"
    t_env.create_temporary_function(
        "record_business_metric",
        udf(BusinessMetricFunction(is_purchase_topic), result_type=DataTypes.STRING()),
    )


def run_insert(t_env: StreamTableEnvironment) -> None:
    t_env.execute_sql("""
        INSERT INTO raw_zone_sink
        SELECT
            event_time, record_business_metric(event_type, price) AS event_type,
            product_id, category_id, category_code,
            brand, price, user_id, user_session, event_id,
            kafka_partition, kafka_offset, kafka_timestamp,
            CURRENT_TIMESTAMP AS ingest_ts,
            DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH') AS raw_datetime
        FROM kafka_source
    """)


def main() -> None:
    args = parse_args()
    t_env = build_table_env(args.checkpoint_interval_ms)
    zone_name = args.topic.rsplit(".", 1)[-1]
    t_env.get_config().set("pipeline.name", f"raw_zone_{zone_name}")
    create_source_table(t_env, args.topic, args.bootstrap_servers)
    create_sink_table(t_env, args.raw_path)
    register_business_metric_udf(t_env, zone_name)
    run_insert(t_env)


if __name__ == "__main__":
    main()
