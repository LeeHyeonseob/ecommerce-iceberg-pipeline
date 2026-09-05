import argparse
import os
from dataclasses import dataclass

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from spark_session import build_spark

load_dotenv()

DEFAULT_S3_BUCKET = os.environ.get("S3_BUCKET")
DEFAULT_AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")

ZONES = ["view", "cart", "purchase"]


@dataclass
class VerifyArgs:
    s3_bucket: str
    aws_region: str
    zones: list[str]


def parse_args() -> VerifyArgs:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=DEFAULT_S3_BUCKET, required=DEFAULT_S3_BUCKET is None)
    parser.add_argument("--aws-region", default=DEFAULT_AWS_REGION)
    parser.add_argument("--zones", default=",".join(ZONES))
    args = parser.parse_args()
    zones = [zone.strip() for zone in args.zones.split(",") if zone.strip()]
    invalid_zones = sorted(set(zones) - set(ZONES))
    if invalid_zones:
        parser.error(f"지원하지 않는 zone: {', '.join(invalid_zones)}")
    if not zones:
        parser.error("최소 한 개의 zone을 지정해야 합니다.")

    return VerifyArgs(
        s3_bucket=args.s3_bucket,
        aws_region=args.aws_region,
        zones=zones,
    )


def read_zone(spark: SparkSession, s3_bucket: str, zone: str) -> DataFrame:
    path = f"s3a://{s3_bucket}/raw/{zone}/"
    df = spark.read.parquet(path)
    # kafka_timestamp(Kafka 실제 수신 시각)와 ingest_ts(Flink 실제 기록 시각)는 둘 다 압축 재생의
    # 영향을 받지 않는 진짜 벽시계 값이라, 이 차이가 순수 파이프라인 처리 지연을 나타냄
    return df.withColumn(
        "pipeline_lag_sec",
        F.col("ingest_ts").cast("double") - F.col("kafka_timestamp").cast("double"),
    )


# 전체 컬럼을 다 뿌리면 옆으로 길어져서 캡처가 안 되므로, 원본 식별자 + 수집 타임스탬프만
SAMPLE_COLUMNS = ["event_id", "user_id", "event_time", "kafka_timestamp", "ingest_ts"]


def print_schema(zone: str, df: DataFrame) -> None:
    print(f"\n{'=' * 60}\nzone={zone}\n{'=' * 60}")
    df.drop("pipeline_lag_sec").printSchema()


def print_sample(zone: str, df: DataFrame) -> None:
    print(f"[{zone}] sample 5건")
    df.select(*SAMPLE_COLUMNS).limit(5).show(truncate=False)


def print_top_lag(zone: str, df: DataFrame) -> None:
    print(f"[{zone}] pipeline_lag_sec 상위 3건")
    (
        df.select("event_id", "kafka_timestamp", "ingest_ts", "pipeline_lag_sec")
        .orderBy(F.col("pipeline_lag_sec").desc())
        .show(3, truncate=False)
    )


def compute_zone_stats(zone: str, df: DataFrame) -> dict:
    row = df.agg(
        F.count("*").alias("row_count"),
        F.avg("pipeline_lag_sec").alias("lag_avg"),
        F.expr("percentile_approx(pipeline_lag_sec, 0.5)").alias("lag_p50"),
        F.expr("percentile_approx(pipeline_lag_sec, 0.95)").alias("lag_p95"),
    ).collect()[0]
    stats = {
        "zone": zone,
        "row_count": row["row_count"],
        "lag_avg": row["lag_avg"],
        "lag_p50": row["lag_p50"],
        "lag_p95": row["lag_p95"],
    }
    print(
        f"[{zone}] row_count={stats['row_count']} "
        f"pipeline_lag_sec(avg={stats['lag_avg']:.4f}, p50={stats['lag_p50']:.4f}, p95={stats['lag_p95']:.4f})"
    )
    return stats


def compare_schemas(dfs: dict[str, DataFrame]) -> None:
    print(f"\n{'=' * 60}\nzone간 스키마 동일성 확인\n{'=' * 60}")
    schemas = {zone: df.drop("pipeline_lag_sec").schema for zone, df in dfs.items()}
    reference_zone, reference_schema = next(iter(schemas.items()))
    all_match = all(schema == reference_schema for schema in schemas.values())
    print(f"모든 zone 스키마 동일 (기준={reference_zone}): {all_match}")
    if not all_match:
        for zone, schema in schemas.items():
            print(f"  {zone}: {schema.simpleString()}")


def print_ratio_comparison(stats_by_zone: dict[str, dict]) -> None:
    print(f"\n{'=' * 60}\nzone별 row count 비율\n{'=' * 60}")
    total = sum(s["row_count"] for s in stats_by_zone.values())
    for zone, stats in stats_by_zone.items():
        pct = (stats["row_count"] / total * 100) if total else 0.0
        print(f"[{zone}] {stats['row_count']}건 ({pct:.2f}%)")

    # 절대 수치를 미리 알고 대조하는 게 아니라(운영 중엔 그런 정답이 없음) 구조적으로 성립해야
    # 하는 관계만 확인. cart vs purchase 대소는 구간에 따라 뒤집히므로(2019-10 전체는 cart>purchase지만
    # 10/1 앞 10시간은 purchase>cart) 단정하지 않고, view 압도만 검증
    if all(z in stats_by_zone for z in ZONES):
        counts = {z: stats_by_zone[z]["row_count"] for z in ZONES}
        others = counts["cart"] + counts["purchase"]
        print(f"view 압도(view > cart + purchase) 성립: {counts['view'] > others}")


def main() -> None:
    args = parse_args()
    spark = build_spark("verify_raw_zones", args.s3_bucket, args.aws_region, log_level="WARN")

    dfs = {zone: read_zone(spark, args.s3_bucket, zone) for zone in args.zones}

    for zone, df in dfs.items():
        print_schema(zone, df)

    compare_schemas(dfs)

    print(f"\n{'=' * 60}\nzone별 수집 지연(pipeline_lag_sec)\n{'=' * 60}")
    stats_by_zone = {}
    for zone, df in dfs.items():
        cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            print_sample(zone, cached_df)
            stats_by_zone[zone] = compute_zone_stats(zone, cached_df)
            print_top_lag(zone, cached_df)
        finally:
            cached_df.unpersist()

    print_ratio_comparison(stats_by_zone)

    spark.stop()


if __name__ == "__main__":
    main()
