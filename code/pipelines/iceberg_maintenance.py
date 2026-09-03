import argparse
import json
import os
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

TABLES = [
    "glue.ecommerce_lakehouse.silver_events",
    "glue.ecommerce_lakehouse.silver_funnel",
    "glue.ecommerce_lakehouse.gold_daily_gmv",
    "glue.ecommerce_lakehouse.gold_funnel_daily",
    "glue.ecommerce_lakehouse.gold_category_gmv",
    "glue.ecommerce_lakehouse.gold_pipeline_sla",
    "glue.ecommerce_lakehouse.gold_data_quality",
]
SNAPSHOT_RETENTION_DAYS = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"), required=os.environ.get("S3_BUCKET") is None)
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--retention-days", type=int, default=SNAPSHOT_RETENTION_DAYS)
    args = parser.parse_args()
    args.tables = [table.strip() for table in args.tables.split(",") if table.strip()]
    if args.retention_days < 1:
        parser.error("--retention-days는 1 이상이어야 합니다")
    return args


def build_spark(s3_bucket: str, aws_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("iceberg_maintenance")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.type", "glue")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{s3_bucket}/warehouse")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.client.region", aws_region)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def procedure_table_name(table: str) -> str:
    return table.removeprefix("glue.")


def rewrite_data_files(spark: SparkSession, table: str) -> dict:
    row = spark.sql(
        f"CALL glue.system.rewrite_data_files(table => '{procedure_table_name(table)}')"
    ).collect()[0]
    return row.asDict()


def expire_snapshots(spark: SparkSession, table: str, older_than: str) -> dict:
    row = spark.sql(
        f"""
        CALL glue.system.expire_snapshots(
            table => '{procedure_table_name(table)}',
            older_than => TIMESTAMP '{older_than}',
            retain_last => 1
        )
        """
    ).collect()[0]
    return row.asDict()


def remove_orphan_files(spark: SparkSession, table: str, older_than: str) -> int:
    # 삭제 파일 전체를 collect하지 않고 건수만 반환한다.
    return spark.sql(
        f"""
        CALL glue.system.remove_orphan_files(
            table => '{procedure_table_name(table)}',
            older_than => TIMESTAMP '{older_than}'
        )
        """
    ).count()


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)
    older_than = (
        datetime.now(timezone.utc) - timedelta(days=args.retention_days)
    ).strftime("%Y-%m-%d %H:%M:%S")
    print(f"보관 기준: {args.retention_days}일 (older_than={older_than})")

    results = {}
    try:
        for table in args.tables:
            print(f"=== {table} ===")
            compact = rewrite_data_files(spark, table)
            expired = expire_snapshots(spark, table, older_than)
            orphan_count = remove_orphan_files(spark, table, older_than)
            results[table] = {
                "rewrite_data_files": compact,
                "expire_snapshots": expired,
                "orphan_count": orphan_count,
            }
            print(f"  compaction={compact} expire={expired} orphan_삭제={orphan_count}건")
    finally:
        spark.stop()

    print(json.dumps(results, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
