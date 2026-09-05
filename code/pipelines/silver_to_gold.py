import argparse
import os

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_session import build_spark

load_dotenv()

# 검증용 환경(test-full 등)을 추가하려면 여기 세 딕셔너리에 항목을 더하고
# 해당 테이블을 만드는 DDL을 같이 준비하면 된다.
EVENTS_TABLES = {
    "prod": "glue.ecommerce_lakehouse.silver_events",
    "test-full": "glue.ecommerce_lakehouse.silver_events_test",
    "test-incremental": "glue.ecommerce_lakehouse.silver_events_test",
}
FUNNEL_TABLES = {
    "prod": "glue.ecommerce_lakehouse.silver_funnel",
    "test-full": "glue.ecommerce_lakehouse.silver_funnel_test_full",
    "test-incremental": "glue.ecommerce_lakehouse.silver_funnel_test_incremental",
}
GOLD_TABLES = {
    env: {
        name: f"glue.ecommerce_lakehouse.{name}"
        + suffix
        for name in (
            "gold_daily_gmv",
            "gold_funnel_daily",
            "gold_category_gmv",
            "gold_pipeline_sla",
            "gold_data_quality",
        )
    }
    for env, suffix in {
        "prod": "",
        "test-full": "_test_full",
        "test-incremental": "_test_incremental",
    }.items()
}

# updated_at을 뺀 DDL 컬럼 순서. 쓰기가 순서를 따지므로 한곳에서 관리한다
GOLD_COLUMNS = {
    "gold_daily_gmv": [
        "summary_date", "gmv", "purchase_cnt", "unique_buyers", "avg_price",
    ],
    "gold_funnel_daily": [
        "summary_date", "category_l1", "funnels", "views", "carts", "purchases",
        "purchases_later", "views_carted", "views_purchased", "carts_purchased",
        "carts_converted_later",
        "view_to_cart", "cart_to_purchase", "view_to_purchase",
        "abandon_rate", "abandon_rate_final", "cart_value", "lost_revenue",
    ],
    "gold_category_gmv": [
        "summary_date", "dim_type", "dim_value", "gmv", "gmv_share",
        "purchase_cnt", "rank_in_day", "cum_gmv_share",
    ],
    "gold_pipeline_sla": [
        "summary_date", "event_hour", "event_type", "events",
        "lag_p50", "lag_p95", "lag_p99", "lag_max",
    ],
    "gold_data_quality": [
        "summary_date", "total_events", "view_cnt", "cart_cnt", "purchase_cnt",
        "cart_view_ratio", "purchase_view_ratio", "null_category_rate", "null_brand_rate",
        "price_null_cnt", "price_nonpositive_cnt", "purchase_without_view_cnt",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--env", choices=list(GOLD_TABLES), default="prod")
    parser.add_argument("--from-date", help="summary_date 하한(포함). 생략 시 전체")
    parser.add_argument("--to-date", help="summary_date 상한(포함). 생략 시 전체")
    parser.add_argument(
        "--event-dates",
        help="증분 전용. 콤마 구분 event_date 목록(연속 아니어도 됨). "
        "--from-date/--to-date와 함께 쓸 수 없음",
    )
    parser.add_argument(
        "--funnel-dates",
        help="증분 전용. 콤마 구분 funnel_date 목록. --from-date/--to-date와 함께 쓸 수 없음",
    )
    parser.add_argument("--tables", default="all", help="쉼표 구분. 생략 시 5개 전부")
    args = parser.parse_args()

    uses_range = bool(args.from_date or args.to_date)
    uses_dates = bool(args.event_dates or args.funnel_dates)
    if uses_range and uses_dates:
        parser.error("--from-date/--to-date와 --event-dates/--funnel-dates는 함께 쓸 수 없습니다")

    args.event_dates = args.event_dates.split(",") if args.event_dates else None
    args.funnel_dates = args.funnel_dates.split(",") if args.funnel_dates else None
    args.events_table = EVENTS_TABLES[args.env]
    args.funnel_table = FUNNEL_TABLES[args.env]
    args.gold_tables = GOLD_TABLES[args.env]
    return args


def _date_filter(df: DataFrame, column: str, from_date, to_date, dates=None) -> DataFrame:
    # 날짜 목록은 비연속일 수 있으므로 IN으로 필터링한다.
    if dates:
        return df.filter(F.col(column).isin(dates))
    if from_date:
        df = df.filter(F.col(column) >= F.lit(from_date).cast("date"))
    if to_date:
        df = df.filter(F.col(column) <= F.lit(to_date).cast("date"))
    return df


def read_events(spark: SparkSession, table: str, from_date, to_date, dates=None) -> DataFrame:
    return _date_filter(spark.table(table), "event_date", from_date, to_date, dates)


def read_funnels(spark: SparkSession, table: str, from_date, to_date, dates=None) -> DataFrame:
    return _date_filter(spark.table(table), "funnel_date", from_date, to_date, dates)


def finalize(df: DataFrame, table_name: str) -> DataFrame:
    return df.withColumn("updated_at", F.current_timestamp()).select(
        *GOLD_COLUMNS[table_name], "updated_at"
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
    return finalize(df, "gold_daily_gmv")


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
    return finalize(df, "gold_category_gmv")


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
    return finalize(df, "gold_pipeline_sla")


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
    return finalize(df, "gold_data_quality")


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
    return finalize(df, "gold_funnel_daily")


def overwrite_partitions(
    df: DataFrame,
    table: str,
    from_date: str | None = None,
    to_date: str | None = None,
    dates: list[str] | None = None,
) -> None:
    # 선택한 날짜만 원자적으로 교체한다. df에 없는 날짜도 조건에 포함하면 해당
    # 파티션은 삭제되므로, 재계산 결과가 0행인 날짜에 오래된 값이 남지 않는다.
    if dates:
        literals = ", ".join(f"DATE '{value}'" for value in dates)
        predicate = f"summary_date IN ({literals})"
    elif from_date or to_date:
        predicates = []
        if from_date:
            predicates.append(f"summary_date >= DATE '{from_date}'")
        if to_date:
            predicates.append(f"summary_date <= DATE '{to_date}'")
        predicate = " AND ".join(predicates)
    else:
        df.writeTo(table).overwritePartitions()
        return
    df.writeTo(table).overwrite(F.expr(predicate))


# (조립 함수, 필요한 소스, 대상 테이블). 소스는 선언 순서대로 인자로 넘어간다
BUILDERS = {
    "gold_daily_gmv":    (build_daily_gmv,    ("events",)),
    "gold_category_gmv": (build_category_gmv, ("events",)),
    "gold_pipeline_sla": (build_pipeline_sla, ("events",)),
    "gold_data_quality": (build_data_quality, ("events", "funnels")),
    "gold_funnel_daily": (build_funnel_daily, ("funnels",)),
}


def main() -> None:
    args = parse_args()
    spark = build_spark("silver_to_gold", args.s3_bucket, args.aws_region)

    targets = list(BUILDERS) if args.tables == "all" else args.tables.split(",")
    unknown = sorted(set(targets) - set(BUILDERS))
    if unknown:
        raise ValueError(f"알 수 없는 Gold 테이블: {unknown}")
    print(f"대상: {targets}")
    print(f"환경: {args.env} events={args.events_table} funnels={args.funnel_table}")
    if args.event_dates or args.funnel_dates:
        print(f"event_dates={args.event_dates} funnel_dates={args.funnel_dates}")
    else:
        print(f"날짜 범위: {args.from_date or '(전체)'} ~ {args.to_date or '(전체)'}")

    readers = {
        "events": lambda from_date, to_date, dates: read_events(
            spark, args.events_table, from_date, to_date, dates
        ),
        "funnels": lambda from_date, to_date, dates: read_funnels(
            spark, args.funnel_table, from_date, to_date, dates
        ),
    }
    dates_by_source = {"events": args.event_dates, "funnels": args.funnel_dates}
    sources: dict[str, DataFrame] = {}

    for name in targets:
        builder, needs = BUILDERS[name]
        table = args.gold_tables[name]

        if name == "gold_data_quality" and (args.event_dates or args.funnel_dates):
            # 두 날짜 축의 합집합을 사용한다.
            dq_dates = sorted(set(args.event_dates or []) | set(args.funnel_dates or []))
            if not dq_dates:
                print(f"{name}: 영향 날짜가 없어 건너뜀")
                continue
            dq_events = read_events(spark, args.events_table, None, None, dq_dates)
            dq_funnels = read_funnels(spark, args.funnel_table, None, None, dq_dates)
            overwrite_partitions(builder(dq_events, dq_funnels), table, dates=dq_dates)
            total = spark.table(table).count()
            print(f"{name}: {len(dq_dates)}개 날짜 갱신, Gold 대상 테이블 전체 행 수={total}")
            continue

        for s in needs:
            if (args.event_dates or args.funnel_dates) and not dates_by_source[s]:
                print(f"{name}: {s} 영향 날짜가 없어 건너뜀")
                break
            if s not in sources:
                sources[s] = readers[s](args.from_date, args.to_date, dates_by_source[s])
        else:
            output_dates = dates_by_source[needs[0]] if (args.event_dates or args.funnel_dates) else None
            overwrite_partitions(
                builder(*(sources[s] for s in needs)),
                table,
                args.from_date,
                args.to_date,
                output_dates,
            )
            scope = output_dates if output_dates is not None else "전체"
            total = spark.table(table).count()
            print(f"{name}: 대상 날짜={scope}, Gold 대상 테이블 전체 행 수={total}")

    spark.stop()


if __name__ == "__main__":
    main()
