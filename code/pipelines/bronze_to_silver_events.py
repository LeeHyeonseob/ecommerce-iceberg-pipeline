import argparse
import json
import os

from datetime import datetime
from functools import reduce

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

load_dotenv()

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

TARGET_TABLES = {
    "prod": "glue.ecommerce_lakehouse.silver_events",
}
ZONES = ["view", "cart", "purchase"]

# Funnel이 읽는 배치 산출물 컬럼 계약.
BATCH_OUTPUT_COLUMNS = [
    "event_id",
    "event_time",
    "event_date",
    "event_type",
    "user_id",
    "user_session",
    "product_id",
]

SILVER_COLUMNS = [
    "event_id",
    "event_time",
    "event_date",
    "event_type",
    "user_id",
    "user_session",
    "product_id",
    "category_id",
    "category_code",
    "category_l1",
    "category_l2",
    "category_l3",
    "brand",
    "price",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "ingest_ts",
    "pipeline_lag_sec",
    "updated_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--env", choices=list(TARGET_TABLES), default="prod")
    parser.add_argument("--zones", default=",".join(ZONES))
    parser.add_argument("--from-datetime", help="raw_datetime 하한(포함). to와 함께 생략 시 전체")
    parser.add_argument("--to-datetime", help="raw_datetime 상한(미포함). from과 함께 생략 시 전체")
    parser.add_argument(
        "--batch-output-path",
        help="지정 시 이번 배치(dedup 후)를 이 S3 경로에 Parquet로 저장. "
        "silver_events_to_funnel.py --mode incremental --batch-input-path가 읽는다. 생략 시 저장 안 함",
    )
    args = parser.parse_args()
    args.target_table = TARGET_TABLES[args.env]

    if bool(args.from_datetime) != bool(args.to_datetime):
        parser.error("--from-datetime과 --to-datetime은 함께 지정해야 합니다")
    if args.from_datetime and args.from_datetime >= args.to_datetime:
        parser.error("--from-datetime은 --to-datetime보다 이전이어야 합니다")
    return args


def build_spark(s3_bucket: str, aws_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("bronze_to_silver_events")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.type", "glue")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{s3_bucket}/warehouse")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.client.region", aws_region)
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_zone(
    spark: SparkSession,
    s3_bucket: str,
    zone: str,
    from_datetime: str | None,
    to_datetime: str | None,
) -> DataFrame:
    df = spark.read.parquet(f"s3a://{s3_bucket}/raw/{zone}/")
    # 시간 파티션으로 후보를 줄인 뒤 ingest_ts로 정확히 필터링한다.
    if from_datetime:
        from_hour = datetime.fromisoformat(from_datetime).strftime("%Y-%m-%d-%H")
        df = df.filter(F.col("raw_datetime") >= from_hour)
        df = df.filter(F.col("ingest_ts") >= F.lit(from_datetime).cast("timestamp"))
    if to_datetime:
        to_hour = datetime.fromisoformat(to_datetime).strftime("%Y-%m-%d-%H")
        # 경계 파티션은 ingest_ts 필터로 제거한다.
        df = df.filter(F.col("raw_datetime") <= to_hour)
        df = df.filter(F.col("ingest_ts") < F.lit(to_datetime).cast("timestamp"))
    return df


def read_bronze(
    spark: SparkSession,
    s3_bucket: str,
    zones: list[str],
    from_datetime: str | None,
    to_datetime: str | None,
) -> DataFrame:
    frames = [read_zone(spark, s3_bucket, z, from_datetime, to_datetime) for z in zones]
    return reduce(DataFrame.unionByName, frames)


def blank_to_null(name: str) -> F.Column:
    return F.when(F.trim(F.col(name)) == "", None).otherwise(F.col(name))


def transform(df: DataFrame) -> DataFrame:
    cleaned = (
        df.withColumn("event_time", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))
        .withColumn("category_code", blank_to_null("category_code"))
        .withColumn("brand", blank_to_null("brand"))
        .withColumn("price", F.col("price").cast("double"))
    )
    parts = F.split(F.col("category_code"), r"\.")
    return (
        cleaned.withColumn("event_date", F.to_date("event_time"))
        .withColumn("category_l1", parts.getItem(0))
        .withColumn("category_l2", parts.getItem(1))
        .withColumn("category_l3", parts.getItem(2))
        .withColumn(
            "pipeline_lag_sec",
            F.col("ingest_ts").cast("double") - F.col("kafka_timestamp").cast("double"),
        )
        .withColumn("updated_at", F.current_timestamp())
        .select(*SILVER_COLUMNS)
    )


def dedup(df: DataFrame) -> DataFrame:
    # dropDuplicates는 어느 행이 남는지 비결정적이라 윈도우 함수를 쓴다
    w = Window.partitionBy("event_id").orderBy(
        F.col("ingest_ts").desc(), F.col("kafka_partition"), F.col("kafka_offset").desc()
    )
    return df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")


def merge_into_silver(spark: SparkSession, df: DataFrame, target_table: str) -> None:
    df.createOrReplaceTempView("batch")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING batch s ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def write_batch_output(df: DataFrame, path: str) -> None:
    # Silver MERGE 성공 후에만 쓴다.
    df.select(*BATCH_OUTPUT_COLUMNS).write.mode("overwrite").parquet(path)


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)

    zones = args.zones.split(",")
    interval = f"[{args.from_datetime}, {args.to_datetime})" if args.from_datetime else "(전체)"
    print(f"raw_datetime 구간: {interval}")

    raw = read_bronze(spark, args.s3_bucket, zones, args.from_datetime, args.to_datetime)
    transformed = transform(raw)
    deduped = dedup(transformed)
    deduped.persist()

    read_count = raw.count()
    stats = deduped.agg(
        F.count("*").alias("n"),
        F.count(F.when(F.col("price").isNull(), 1)).alias("null_price"),
        F.sort_array(F.collect_set("event_date")).alias("event_dates"),
    ).collect()[0]

    merge_into_silver(spark, deduped, args.target_table)

    if args.batch_output_path:
        if stats["n"] > 0:
            write_batch_output(deduped, args.batch_output_path)
            print(f"배치 산출물: {args.batch_output_path} (event_count={stats['n']})")
        else:
            # 빈 Parquet는 쓰지 않고 후속 작업을 건너뛴다.
            print(f"배치가 비어 있어 산출물을 쓰지 않음 (event_count=0, path={args.batch_output_path})")

    print(f"읽은 행={read_count} dedup 후={stats['n']} (접힌 행={read_count - stats['n']})")
    print(f"price NULL={stats['null_price']}")

    deduped.unpersist()
    spark.stop()
    print(json.dumps({
        "batch_output_path": args.batch_output_path if stats["n"] > 0 else None,
        "event_count": stats["n"],
        "event_dates": [str(value) for value in stats["event_dates"]],
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
