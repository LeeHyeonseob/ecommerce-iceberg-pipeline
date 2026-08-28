import argparse
import os

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

TARGET_TABLE = "glue.ecommerce_lakehouse.silver_events"
ZONES = ["view", "cart", "purchase"]

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
    parser.add_argument("--zones", default=",".join(ZONES))
    parser.add_argument("--from-datetime", help="raw_datetime 하한(포함). 생략 시 타깃 테이블에서 자동 계산")
    parser.add_argument("--to-datetime", help="raw_datetime 상한(포함). 생략 시 제한 없음")
    return parser.parse_args()


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


def resolve_watermark(spark: SparkSession, zones: list[str], override: str | None) -> dict[str, str]:
    if override:
        return {z: override for z in zones}
    rows = spark.sql(f"""
        SELECT event_type, date_format(max(ingest_ts), 'yyyy-MM-dd-HH') AS watermark
        FROM {TARGET_TABLE}
        GROUP BY event_type
    """).collect()
    return {r["event_type"]: r["watermark"] for r in rows if r["watermark"]}


def read_zone(
    spark: SparkSession,
    s3_bucket: str,
    zone: str,
    from_datetime: str | None,
    to_datetime: str | None,
) -> DataFrame:
    df = spark.read.parquet(f"s3a://{s3_bucket}/raw/{zone}/")
    # 경계 시간대를 Flink가 아직 쓰는 중일 수 있어 > 가 아니라 >= 다
    if from_datetime:
        df = df.filter(F.col("raw_datetime") >= from_datetime)
    if to_datetime:
        df = df.filter(F.col("raw_datetime") <= to_datetime)
    return df


def read_bronze(
    spark: SparkSession,
    s3_bucket: str,
    zones: list[str],
    watermarks: dict[str, str],
    to_datetime: str | None,
) -> DataFrame:
    frames = [read_zone(spark, s3_bucket, z, watermarks.get(z), to_datetime) for z in zones]
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


def merge_into_silver(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("batch")
    spark.sql(
        f"""
        MERGE INTO {TARGET_TABLE} t
        USING batch s ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)

    zones = args.zones.split(",")
    watermarks = resolve_watermark(spark, zones, args.from_datetime)
    print(f"zone별 하한: {watermarks or '(전체)'} / 상한: {args.to_datetime or '(제한 없음)'}")

    raw = read_bronze(spark, args.s3_bucket, zones, watermarks, args.to_datetime)
    transformed = transform(raw)
    deduped = dedup(transformed)

    read_count = raw.count()
    stats = deduped.agg(
        F.count("*").alias("n"),
        F.count(F.when(F.col("price").isNull(), 1)).alias("null_price"),
    ).collect()[0]

    merge_into_silver(spark, deduped)

    total = spark.table(TARGET_TABLE).count()
    print(f"읽은 행={read_count} dedup 후={stats['n']} (접힌 행={read_count - stats['n']})")
    print(f"price NULL={stats['null_price']}")
    print(f"{TARGET_TABLE} 전체={total}")

    spark.stop()


if __name__ == "__main__":
    main()
