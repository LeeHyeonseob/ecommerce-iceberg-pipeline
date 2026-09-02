import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

ICEBERG_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
])

DDL_DIR = Path(__file__).resolve().parents[1] / "ddl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--files", default="*.sql")
    return parser.parse_args()


def build_spark(s3_bucket: str, aws_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("run_ddl")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", ICEBERG_PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.type", "glue")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{s3_bucket}/warehouse")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.client.region", aws_region)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def split_statements(sql: str) -> list[str]:
    # 주석 줄을 제거한 뒤 세미콜론으로 분리 (문자열 리터럴 안의 세미콜론은 쓰지 않음)
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)

    for path in sorted(DDL_DIR.glob(args.files)):
        print(f"\n=== {path.name} ===")
        for stmt in split_statements(path.read_text()):
            head = " ".join(stmt.split())[:70]
            print(f"  {head}...")
            spark.sql(stmt)
        print(f"  OK")

    print("\n=== 생성된 테이블 ===")
    spark.sql("SHOW TABLES IN glue.ecommerce_lakehouse").show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
