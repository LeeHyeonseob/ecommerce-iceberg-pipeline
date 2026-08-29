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

EVENTS_TABLE = "glue.ecommerce_lakehouse.silver_events"
FUNNEL_TABLE = "glue.ecommerce_lakehouse.silver_funnel"

GOLD_DAILY_GMV    = "glue.ecommerce_lakehouse.gold_daily_gmv"
GOLD_FUNNEL_DAILY = "glue.ecommerce_lakehouse.gold_funnel_daily"
GOLD_CATEGORY_GMV = "glue.ecommerce_lakehouse.gold_category_gmv"
GOLD_PIPELINE_SLA = "glue.ecommerce_lakehouse.gold_pipeline_sla"
GOLD_DATA_QUALITY = "glue.ecommerce_lakehouse.gold_data_quality"

# updated_at을 뺀 DDL 컬럼 순서. 쓰기가 순서를 따지므로 한곳에서 관리한다
GOLD_COLUMNS = {
    GOLD_DAILY_GMV: [
        "summary_date", "gmv", "purchase_cnt", "unique_buyers", "avg_price",
    ],
    GOLD_FUNNEL_DAILY: [
        "summary_date", "category_l1", "funnels", "views", "carts", "purchases",
        "purchases_later", "views_carted", "views_purchased", "carts_purchased",
        "carts_converted_later",
        "view_to_cart", "cart_to_purchase", "view_to_purchase",
        "abandon_rate", "abandon_rate_final", "cart_value", "lost_revenue",
    ],
    GOLD_CATEGORY_GMV: [
        "summary_date", "dim_type", "dim_value", "gmv", "gmv_share",
        "purchase_cnt", "rank_in_day", "cum_gmv_share",
    ],
    GOLD_PIPELINE_SLA: [
        "summary_date", "event_hour", "event_type", "events",
        "lag_p50", "lag_p95", "lag_p99", "lag_max",
    ],
    GOLD_DATA_QUALITY: [
        "summary_date", "total_events", "view_cnt", "cart_cnt", "purchase_cnt",
        "cart_view_ratio", "purchase_view_ratio", "null_category_rate", "null_brand_rate",
        "price_null_cnt", "price_nonpositive_cnt", "purchase_without_view_cnt",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--from-date", help="summary_date 하한(포함). 생략 시 전체")
    parser.add_argument("--to-date", help="summary_date 상한(포함). 생략 시 전체")
    parser.add_argument("--tables", default="all", help="쉼표 구분. 생략 시 5개 전부")
    return parser.parse_args()


def build_spark(s3_bucket: str, aws_region: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName("silver_to_gold")
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


def _date_filter(df: DataFrame, column: str, from_date, to_date) -> DataFrame:
    if from_date:
        df = df.filter(F.col(column) >= F.lit(from_date).cast("date"))
    if to_date:
        df = df.filter(F.col(column) <= F.lit(to_date).cast("date"))
    return df


def read_events(spark: SparkSession, from_date, to_date) -> DataFrame:
    return _date_filter(spark.table(EVENTS_TABLE), "event_date", from_date, to_date)


def read_funnels(spark: SparkSession, from_date, to_date) -> DataFrame:
    return _date_filter(spark.table(FUNNEL_TABLE), "funnel_date", from_date, to_date)


def finalize(df: DataFrame, table: str) -> DataFrame:
    return df.withColumn("updated_at", F.current_timestamp()).select(
        *GOLD_COLUMNS[table], "updated_at"
    )


def build_daily_gmv(events: DataFrame) -> DataFrame:
    # 퍼널에서 뽑으면 중복 구매가 접혀 GMV가 7.1% 과소집계된다
    events.createOrReplaceTempView("gmv_source_events")
    df = events.sparkSession.sql("""
        SELECT
            event_date              AS summary_date,
            sum(price)              AS gmv,
            count(*)                AS purchase_cnt,
            count(DISTINCT user_id) AS unique_buyers,
            avg(price)              AS avg_price
        FROM gmv_source_events
        WHERE event_type = 'purchase'
        GROUP BY event_date
    """)
    return finalize(df, GOLD_DAILY_GMV)


def build_category_gmv(events: DataFrame) -> DataFrame:
    # NULL을 'unknown'으로 살리지 않으면 gmv_share 합이 1이 안 된다.
    # rank가 아니라 row_number인 건 동점일 때 cum_gmv_share와 어긋나지 않게 하려는 것
    events.createOrReplaceTempView("cat_source_events")
    df = events.sparkSession.sql("""
        WITH purchases AS (
            SELECT
                event_date,
                price,
                coalesce(category_l1, 'unknown') AS category_l1,
                coalesce(brand, 'unknown')       AS brand,
                product_id
            FROM cat_source_events
            WHERE event_type = 'purchase'
        ),
        stacked AS (
            SELECT event_date, 'category_l1' AS dim_type, category_l1 AS dim_value, price FROM purchases
            UNION ALL
            SELECT event_date, 'brand',       brand,      price FROM purchases
            UNION ALL
            SELECT event_date, 'product',     product_id, price FROM purchases
        ),
        agg AS (
            SELECT
                event_date AS summary_date,
                dim_type,
                dim_value,
                sum(price) AS gmv,
                count(*)   AS purchase_cnt
            FROM stacked
            GROUP BY event_date, dim_type, dim_value
        )
        SELECT
            summary_date,
            dim_type,
            dim_value,
            gmv,
            gmv / sum(gmv) OVER (PARTITION BY summary_date, dim_type) AS gmv_share,
            purchase_cnt,
            int(row_number() OVER (
                PARTITION BY summary_date, dim_type ORDER BY gmv DESC, dim_value
            )) AS rank_in_day,
            sum(gmv) OVER (
                PARTITION BY summary_date, dim_type ORDER BY gmv DESC, dim_value
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) / sum(gmv) OVER (PARTITION BY summary_date, dim_type) AS cum_gmv_share
        FROM agg
    """)
    return finalize(df, GOLD_CATEGORY_GMV)


def build_pipeline_sla(events: DataFrame) -> DataFrame:
    # 지금 데이터는 벌크 적재라 lag가 전부 0.003초대다. 분위수가 의미를 갖지 못한다
    events.createOrReplaceTempView("sla_source_events")
    df = events.sparkSession.sql("""
        SELECT
            event_date          AS summary_date,
            int(hour(event_time)) AS event_hour,
            event_type,
            count(*)            AS events,
            percentile_approx(pipeline_lag_sec, 0.50) AS lag_p50,
            percentile_approx(pipeline_lag_sec, 0.95) AS lag_p95,
            percentile_approx(pipeline_lag_sec, 0.99) AS lag_p99,
            max(pipeline_lag_sec)                     AS lag_max
        FROM sla_source_events
        GROUP BY event_date, hour(event_time), event_type
    """)
    return finalize(df, GOLD_PIPELINE_SLA)


def build_data_quality(events: DataFrame, funnels: DataFrame) -> DataFrame:
    # purchase_without_view_cnt는 퍼널로 접은 뒤에야 보여서 소스가 둘이다.
    # 날짜 축이 달라(event_date vs funnel_date) 각각 집계한 뒤 조인한다
    events.createOrReplaceTempView("dq_source_events")
    funnels.createOrReplaceTempView("dq_source_funnels")
    df = events.sparkSession.sql("""
        WITH e AS (
            SELECT
                event_date AS summary_date,
                count(*)                                        AS total_events,
                count(*) FILTER (WHERE event_type = 'view')      AS view_cnt,
                count(*) FILTER (WHERE event_type = 'cart')      AS cart_cnt,
                count(*) FILTER (WHERE event_type = 'purchase')  AS purchase_cnt,
                count(*) FILTER (WHERE category_code IS NULL)    AS null_category_cnt,
                count(*) FILTER (WHERE brand IS NULL)            AS null_brand_cnt,
                count(*) FILTER (WHERE price IS NULL)            AS price_null_cnt,
                count(*) FILTER (WHERE price <= 0)               AS price_nonpositive_cnt
            FROM dq_source_events
            GROUP BY event_date
        ),
        f AS (
            SELECT
                funnel_date AS summary_date,
                count(*) FILTER (WHERE viewed = 0 AND purchased = 1) AS purchase_without_view_cnt
            FROM dq_source_funnels
            GROUP BY funnel_date
        )
        SELECT
            e.summary_date,
            e.total_events,
            e.view_cnt,
            e.cart_cnt,
            e.purchase_cnt,
            e.cart_cnt         / nullif(e.view_cnt, 0)    AS cart_view_ratio,
            e.purchase_cnt     / nullif(e.view_cnt, 0)    AS purchase_view_ratio,
            e.null_category_cnt / nullif(e.total_events, 0) AS null_category_rate,
            e.null_brand_cnt    / nullif(e.total_events, 0) AS null_brand_rate,
            e.price_null_cnt,
            e.price_nonpositive_cnt,
            coalesce(f.purchase_without_view_cnt, 0)      AS purchase_without_view_cnt
        FROM e
        LEFT JOIN f ON e.summary_date = f.summary_date
    """)
    return finalize(df, GOLD_DATA_QUALITY)


def build_funnel_daily(funnels: DataFrame) -> DataFrame:
    # NULL을 'unknown'으로 먼저 바꾼 뒤 grouping()으로 롤업 행을 식별한다.
    # 순서가 반대면 카테고리 없는 33.88%가 'ALL'로 둔갑하고 에러도 안 난다.
    # 비율은 분자가 분모의 부분집합이 되게 짠다 - 아니면 100%를 넘는다(실제로 110%였다)
    funnels.createOrReplaceTempView("funnel_daily_source")
    df = funnels.sparkSession.sql("""
        WITH f AS (
            SELECT
                funnel_date,
                coalesce(category_l1, 'unknown') AS category_l1,
                viewed, carted, purchased, converted_later, cart_price
            FROM funnel_daily_source
        )
        SELECT
            funnel_date AS summary_date,
            CASE WHEN grouping(category_l1) = 1 THEN 'ALL' ELSE category_l1 END AS category_l1,

            count(*)             AS funnels,
            sum(viewed)          AS views,
            sum(carted)          AS carts,
            sum(purchased)       AS purchases,
            sum(converted_later) AS purchases_later,

            count(*) FILTER (WHERE viewed = 1 AND carted = 1)     AS views_carted,
            count(*) FILTER (WHERE viewed = 1 AND purchased = 1)  AS views_purchased,
            count(*) FILTER (WHERE carted = 1 AND purchased = 1)  AS carts_purchased,
            count(*) FILTER (WHERE carted = 1 AND purchased = 0
                                   AND converted_later = 1)       AS carts_converted_later,

            count(*) FILTER (WHERE viewed = 1 AND carted = 1)
                           / nullif(sum(viewed), 0) AS view_to_cart,
            count(*) FILTER (WHERE carted = 1 AND purchased = 1)
                           / nullif(sum(carted), 0) AS cart_to_purchase,
            count(*) FILTER (WHERE viewed = 1 AND purchased = 1)
                           / nullif(sum(viewed), 0) AS view_to_purchase,

            count(*) FILTER (WHERE carted = 1 AND purchased = 0)
                           / nullif(sum(carted), 0) AS abandon_rate,
            count(*) FILTER (WHERE carted = 1 AND purchased = 0 AND converted_later = 0)
                           / nullif(sum(carted), 0) AS abandon_rate_final,

            sum(cart_price) FILTER (WHERE carted = 1) AS cart_value,
            sum(cart_price) FILTER (WHERE carted = 1 AND purchased = 0
                                          AND converted_later = 0) AS lost_revenue
        FROM f
        GROUP BY GROUPING SETS ((funnel_date, category_l1), (funnel_date))
    """)
    return finalize(df, GOLD_FUNNEL_DAILY)


def overwrite_partitions(df: DataFrame, table: str) -> None:
    # 배치에 들어있는 파티션만 교체. append면 같은 날짜 재실행 시 행이 두 배가 된다
    df.writeTo(table).overwritePartitions()


# (조립 함수, 필요한 소스, 대상 테이블). 소스는 선언 순서대로 인자로 넘어간다
BUILDERS = {
    "gold_daily_gmv":    (build_daily_gmv,    ("events",),           GOLD_DAILY_GMV),
    "gold_category_gmv": (build_category_gmv, ("events",),           GOLD_CATEGORY_GMV),
    "gold_pipeline_sla": (build_pipeline_sla, ("events",),           GOLD_PIPELINE_SLA),
    "gold_data_quality": (build_data_quality, ("events", "funnels"), GOLD_DATA_QUALITY),
    "gold_funnel_daily": (build_funnel_daily, ("funnels",),          GOLD_FUNNEL_DAILY),
}


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)

    targets = list(BUILDERS) if args.tables == "all" else args.tables.split(",")
    print(f"대상: {targets}")
    print(f"날짜 범위: {args.from_date or '(전체)'} ~ {args.to_date or '(전체)'}")

    readers = {"events": read_events, "funnels": read_funnels}
    sources: dict[str, DataFrame] = {}

    for name in targets:
        builder, needs, table = BUILDERS[name]
        for s in needs:
            if s not in sources:
                sources[s] = readers[s](spark, args.from_date, args.to_date)

        overwrite_partitions(builder(*(sources[s] for s in needs)), table)
        print(f"{name}: {spark.table(table).count()}행")

    spark.stop()


if __name__ == "__main__":
    main()
