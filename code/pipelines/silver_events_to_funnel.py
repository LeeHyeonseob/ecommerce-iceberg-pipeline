import argparse
import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

load_dotenv()

PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2",
    "org.apache.iceberg:iceberg-aws-bundle:1.9.2",
])

SOURCE_TABLE = "glue.ecommerce_lakehouse.silver_events"
TARGET_TABLE = "glue.ecommerce_lakehouse.silver_funnel"

CROSS_SESSION_WINDOW_DAYS = 30

FUNNEL_COLUMNS = [
    "user_session",
    "user_id",
    "product_id",
    "funnel_date",
    "viewed",
    "carted",
    "purchased",
    "first_view_ts",
    "first_cart_ts",
    "first_purchase_ts",
    "view_count",
    "cart_count",
    "purchase_count",
    "view_to_cart_sec",
    "cart_to_purchase_sec",
    "view_to_purchase_sec",
    "converted_later",
    "later_purchase_ts",
    "later_purchase_gap_sec",
    "cart_price",
    "purchase_price",
    "category_l1",
    "brand",
    "updated_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--from-date", help="event_date 하한(포함). 생략 시 전체")
    parser.add_argument("--to-date", help="event_date 상한(포함). 생략 시 전체")
    return parser.parse_args()


def build_spark(s3_bucket: str, aws_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("silver_events_to_funnel")
        .config("spark.driver.memory", "8g")
        .config("spark.jars.packages", PACKAGES)
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


def read_events(spark: SparkSession, from_date: str | None, to_date: str | None) -> DataFrame:
    df = spark.table(SOURCE_TABLE)
    if from_date:
        df = df.filter(F.col("event_date") >= F.lit(from_date).cast("date"))
    if to_date:
        df = df.filter(F.col("event_date") <= F.lit(to_date).cast("date"))
    return df


def build_session_funnel(events: DataFrame) -> DataFrame:
    events.createOrReplaceTempView("funnel_source_events")
    return events.sparkSession.sql("""
        SELECT
            user_session,
            user_id,
            product_id,
            to_date(coalesce(first_view_ts, first_cart_ts, first_purchase_ts)) AS funnel_date,

            CASE WHEN view_count     > 0 THEN 1 ELSE 0 END AS viewed,
            CASE WHEN cart_count     > 0 THEN 1 ELSE 0 END AS carted,
            CASE WHEN purchase_count > 0 THEN 1 ELSE 0 END AS purchased,

            first_view_ts,
            first_cart_ts,
            first_purchase_ts,

            int(view_count)     AS view_count,
            int(cart_count)     AS cart_count,
            int(purchase_count) AS purchase_count,

            bigint(first_cart_ts)     - bigint(first_view_ts) AS view_to_cart_sec,
            bigint(first_purchase_ts) - bigint(first_cart_ts) AS cart_to_purchase_sec,
            bigint(first_purchase_ts) - bigint(first_view_ts) AS view_to_purchase_sec,

            cart_price,
            purchase_price,
            category_l1,
            brand
        FROM (
            SELECT
                user_session,
                product_id,
                min_by(user_id, event_time) AS user_id,

                min(event_time) FILTER (WHERE event_type = 'view')     AS first_view_ts,
                min(event_time) FILTER (WHERE event_type = 'cart')     AS first_cart_ts,
                min(event_time) FILTER (WHERE event_type = 'purchase') AS first_purchase_ts,

                count(*) FILTER (WHERE event_type = 'view')     AS view_count,
                count(*) FILTER (WHERE event_type = 'cart')     AS cart_count,
                count(*) FILTER (WHERE event_type = 'purchase') AS purchase_count,

                min_by(price, event_time) FILTER (WHERE event_type = 'cart')     AS cart_price,
                min_by(price, event_time) FILTER (WHERE event_type = 'purchase') AS purchase_price,

                max(category_l1) AS category_l1,
                max(brand)       AS brand
            FROM funnel_source_events
            GROUP BY user_session, product_id
        )
    """)


def attach_cross_session(funnels: DataFrame, events: DataFrame) -> DataFrame:
    spark = funnels.sparkSession
    funnels.createOrReplaceTempView("funnel_stage1")
    (
        events.filter(F.col("event_type") == "purchase")
        .select("user_id", "product_id", "user_session", "event_time")
        .createOrReplaceTempView("funnel_purchases")
    )
    return spark.sql(f"""
        WITH later AS (
            SELECT
                f.user_session,
                f.product_id,
                min(p.event_time) AS later_purchase_ts
            FROM funnel_stage1 f
            JOIN funnel_purchases p
              ON  f.user_id      = p.user_id
              AND f.product_id   = p.product_id
              AND p.user_session <> f.user_session
              AND p.event_time >  coalesce(f.first_cart_ts, f.first_view_ts)
              AND p.event_time <= coalesce(f.first_cart_ts, f.first_view_ts)
                                  + INTERVAL {CROSS_SESSION_WINDOW_DAYS} DAYS
            WHERE f.purchased = 0
            GROUP BY f.user_session, f.product_id
        )
        SELECT
            f.*,
            CASE WHEN l.later_purchase_ts IS NOT NULL THEN 1 ELSE 0 END AS converted_later,
            l.later_purchase_ts,
            bigint(l.later_purchase_ts)
              - bigint(coalesce(f.first_cart_ts, f.first_view_ts)) AS later_purchase_gap_sec,
            current_timestamp() AS updated_at
        FROM funnel_stage1 f
        LEFT JOIN later l
          ON f.user_session = l.user_session AND f.product_id = l.product_id
    """).select(*FUNNEL_COLUMNS)


def merge_into_funnel(spark: SparkSession, df: DataFrame) -> None:
    df.createOrReplaceTempView("silver_funnel_batch")
    spark.sql(f"""
        MERGE INTO {TARGET_TABLE} t
        USING silver_funnel_batch s
          ON t.user_session = s.user_session AND t.product_id = s.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)

    print(f"event_date 범위: {args.from_date or '(전체)'} ~ {args.to_date or '(전체)'}")

    events = read_events(spark, args.from_date, args.to_date)
    session_funnel = build_session_funnel(events)
    funnel = attach_cross_session(session_funnel, events)

    stats = funnel.agg(
        F.count("*").alias("funnels"),
        F.sum("viewed").alias("viewed"),
        F.sum("carted").alias("carted"),
        F.sum("purchased").alias("purchased"),
        F.sum("converted_later").alias("converted_later"),
    ).collect()[0]

    merge_into_funnel(spark, funnel)

    total = spark.table(TARGET_TABLE).count()
    print(f"퍼널={stats['funnels']} viewed={stats['viewed']} carted={stats['carted']}")
    print(f"세션 내 전환={stats['purchased']} 지연 전환={stats['converted_later']}")
    print(f"{TARGET_TABLE} 전체={total}")

    spark.stop()


if __name__ == "__main__":
    main()
