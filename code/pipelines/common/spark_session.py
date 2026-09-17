"""파이프라인 공용 SparkSession 빌더.

5개 스크립트에 복사돼 있던 build_spark()를 하나로 합친 것. 잡별로 다른 건
appName과 로그 레벨뿐이고, 나머지 설정은 전부 동일하게 맞춘다.
"""
import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
    # Spark가 번들한 Hadoop 버전과 맞춰야 함 (Spark 3.5.x → Hadoop 3.3.4).
    # Bronze/배치 Parquet의 s3a:// 직접 읽기와 아래 fs.s3.impl 매핑에 필요하다.
    "org.apache.hadoop:hadoop-aws:3.3.4",
])

DEFAULT_DRIVER_MEMORY = "8g"


def build_spark(
    app_name: str,
    s3_bucket: str,
    aws_region: str,
    *,
    log_level: str = "ERROR",
) -> SparkSession:
    """Glue 카탈로그 + S3A가 설정된 SparkSession을 만든다.

    log_level: 파이프라인 잡은 ERROR(출력 마지막 줄의 JSON만 Airflow가 파싱한다),
    운영 도구(iceberg_maintenance, health_check, verify_raw_zones)는 WARN.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", DEFAULT_DRIVER_MEMORY))
        .config("spark.jars.packages", PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.type", "glue")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{s3_bucket}/warehouse")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.client.region", aws_region)
        # Iceberg remove_orphan_files는 테이블 location을 Hadoop FileSystem으로 직접
        # 리스팅하는데, warehouse가 s3:// 스킴이라 이 매핑이 없으면
        # UnsupportedFileSystemException: No FileSystem for scheme "s3" 로 죽는다.
        # (Hadoop 3.x는 s3 스킴 구현을 제거해서 core-default.xml에 기본값이 없다.)
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # 없으면 S3A가 글로벌 엔드포인트(s3.amazonaws.com)로 붙어 리다이렉트에 의존한다.
        # 서울 리전은 그렇게도 동작하지만, hadoop-aws 3.4.x(SDK v2)나 opt-in 리전에서 깨진다.
        .config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark
