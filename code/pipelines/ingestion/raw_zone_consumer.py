import argparse
from decimal import Decimal, InvalidOperation

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, Row, StreamTableEnvironment
from pyflink.table.udf import ScalarFunction, udf

from pipelines.common.event_contract import ALLOWED_TOPICS, DLQ_TOPIC
from pipelines.common.event_validation import ValidationResult, validate_event

DLQ_FAILURE_STAGE = "FLINK_VALIDATION"

PARSED_ROW_TYPE = DataTypes.ROW([
    DataTypes.FIELD("is_valid", DataTypes.BOOLEAN()),
    *(DataTypes.FIELD(name, DataTypes.STRING()) for name in ValidationResult._fields[1:]),
])


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


class ParseAndValidate(ScalarFunction):
    """순수 검증 결과를 PyFlink Row로 변환한다."""

    def __init__(self, expected_event_type: str):
        self.expected_event_type = expected_event_type

    def eval(self, payload):
        return Row(*validate_event(payload, self.expected_event_type))


class RecordDlqMetric(ScalarFunction):
    """DLQ로 빠진 레코드 수만 세는 부수효과 UDF. DLQ 분기 SELECT에서만 호출한다."""

    def open(self, function_context):
        metrics = function_context.get_metric_group().add_group("business")
        self.dlq_counter = metrics.counter("dlq_count")

    def eval(self, reason_code):
        self.dlq_counter.inc()
        return reason_code


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, choices=ALLOWED_TOPICS)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--raw-path", required=True)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30_000)
    return parser.parse_args()


def build_table_env(checkpoint_interval_ms: int) -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(checkpoint_interval_ms)
    t_env = StreamTableEnvironment.create(env)
    # StatementSet의 두 INSERT가 동일한 Kafka source를 공유하도록 명시한다.
    t_env.get_config().set("table.optimizer.reuse-sub-plan-enabled", "true")
    t_env.get_config().set("table.optimizer.reuse-source-enabled", "true")
    return t_env


def create_source_table(t_env: StreamTableEnvironment, topic: str, bootstrap_servers: str) -> None:
    consumer_group = f"raw-zone-consumer-{topic.replace('.', '-')}"
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            payload STRING,
            kafka_partition INT METADATA FROM 'partition' VIRTUAL,
            kafka_offset BIGINT METADATA FROM 'offset' VIRTUAL,
            kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp' VIRTUAL
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{topic}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'properties.group.id' = '{consumer_group}',
            'format' = 'raw',
            'scan.startup.mode' = 'group-offsets',
            'properties.auto.offset.reset' = 'earliest'
        )
    """)


def create_sink_tables(t_env: StreamTableEnvironment, raw_path: str, bootstrap_servers: str) -> None:
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

    t_env.execute_sql(f"""
        CREATE TABLE dlq_sink (
            original_payload STRING,
            original_topic STRING,
            kafka_partition INT,
            kafka_offset BIGINT,
            kafka_timestamp TIMESTAMP_LTZ(3),
            failure_stage STRING,
            reason_code STRING,
            failure_detail STRING,
            failed_at TIMESTAMP_LTZ(3)
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{DLQ_TOPIC}',
            'properties.bootstrap.servers' = '{bootstrap_servers}',
            'format' = 'json',
            'sink.delivery-guarantee' = 'at-least-once'
        )
    """)


def register_functions(t_env: StreamTableEnvironment, zone_name: str) -> None:
    is_purchase_topic = zone_name == "purchase"
    t_env.create_temporary_function(
        "record_business_metric",
        udf(BusinessMetricFunction(is_purchase_topic), result_type=DataTypes.STRING()),
    )
    t_env.create_temporary_function(
        "parse_and_validate",
        udf(ParseAndValidate(zone_name), result_type=PARSED_ROW_TYPE),
    )
    t_env.create_temporary_function(
        "record_dlq_metric",
        udf(RecordDlqMetric(), result_type=DataTypes.STRING()),
    )


def run_inserts(t_env: StreamTableEnvironment, topic: str) -> None:
    t_env.execute_sql("""
        CREATE TEMPORARY VIEW parsed_source AS
        SELECT
            payload, kafka_partition, kafka_offset, kafka_timestamp,
            parse_and_validate(payload) AS v
        FROM kafka_source
    """)

    stmt_set = t_env.create_statement_set()
    stmt_set.add_insert_sql("""
        INSERT INTO raw_zone_sink
        SELECT
            v.event_time, record_business_metric(v.event_type, v.price) AS event_type,
            v.product_id, v.category_id, v.category_code,
            v.brand, v.price, v.user_id, v.user_session, v.event_id,
            kafka_partition, kafka_offset, kafka_timestamp,
            CURRENT_TIMESTAMP AS ingest_ts,
            DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd-HH') AS raw_datetime
        FROM parsed_source WHERE v.is_valid
    """)
    stmt_set.add_insert_sql(f"""
        INSERT INTO dlq_sink
        SELECT
            payload, '{topic}' AS original_topic, kafka_partition, kafka_offset, kafka_timestamp,
            '{DLQ_FAILURE_STAGE}' AS failure_stage,
            record_dlq_metric(v.reason_code) AS reason_code,
            v.failure_detail,
            CURRENT_TIMESTAMP AS failed_at
        FROM parsed_source WHERE NOT v.is_valid
    """)
    stmt_set.execute()


def main() -> None:
    args = parse_args()
    t_env = build_table_env(args.checkpoint_interval_ms)
    zone_name = args.topic.rsplit(".", 1)[-1]
    t_env.get_config().set("pipeline.name", f"raw_zone_{zone_name}")
    create_source_table(t_env, args.topic, args.bootstrap_servers)
    create_sink_tables(t_env, args.raw_path, args.bootstrap_servers)
    register_functions(t_env, zone_name)
    run_inserts(t_env, args.topic)


if __name__ == "__main__":
    main()
