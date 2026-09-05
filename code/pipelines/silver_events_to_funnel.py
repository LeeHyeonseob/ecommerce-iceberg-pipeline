import argparse
import json
import os
from datetime import timedelta

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from spark_session import build_spark

load_dotenv()

# 환경별 테이블을 고정 매핑해 오기입을 막는다.
SOURCE_TABLES = {
    "prod": "glue.ecommerce_lakehouse.silver_events",
    "test-full": "glue.ecommerce_lakehouse.silver_events_test",
    "test-incremental": "glue.ecommerce_lakehouse.silver_events_test",
}
TARGET_TABLES = {
    "prod": "glue.ecommerce_lakehouse.silver_funnel",
    "test-full": "glue.ecommerce_lakehouse.silver_funnel_test_full",
    "test-incremental": "glue.ecommerce_lakehouse.silver_funnel_test_incremental",
}

CROSS_SESSION_WINDOW_DAYS = 30

BATCH_EVENT_COLUMNS = [
    "event_id",
    "event_time",
    "event_date",
    "event_type",
    "user_id",
    "user_session",
    "product_id",
]

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
    parser.add_argument("--mode", choices=["full", "incremental"], required=True)
    parser.add_argument(
        "--env",
        choices=list(TARGET_TABLES),
        default="prod",
        help="쓸 대상 — 고정된 테이블명으로만 매핑됨(TARGET_TABLES 참고)",
    )
    parser.add_argument("--from-date", help="[full 전용] event_date 하한(포함). 생략 시 전체")
    parser.add_argument("--to-date", help="[full 전용] event_date 상한(포함). 생략 시 전체")
    parser.add_argument(
        "--batch-input-path",
        help="[incremental 전용] bronze_to_silver_events가 낸 배치 산출물 경로",
    )
    args = parser.parse_args()
    args.source_table = SOURCE_TABLES[args.env]
    args.target_table = TARGET_TABLES[args.env]

    if args.mode == "full":
        if args.batch_input_path:
            parser.error("--mode full에서는 --batch-input-path를 쓸 수 없습니다")
    else:
        if not args.batch_input_path:
            parser.error("--mode incremental에는 --batch-input-path가 필요합니다")
        if args.from_date or args.to_date:
            parser.error("--mode incremental에서는 --from-date/--to-date를 쓸 수 없습니다")

    return args


def read_events(
    spark: SparkSession, source_table: str, from_date: str | None, to_date: str | None
) -> DataFrame:
    df = spark.table(source_table)
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


def attach_cross_session(funnels: DataFrame, purchase_evidence: DataFrame) -> DataFrame:
    # purchase_evidence는 purchase만 담은 공통 입력이다.
    spark = funnels.sparkSession
    funnels.createOrReplaceTempView("funnel_stage1")
    (
        purchase_evidence
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


def merge_into_funnel(
    spark: SparkSession, df: DataFrame, target_table: str, funnel_dates: set | None = None
) -> None:
    df.createOrReplaceTempView("silver_funnel_batch")
    date_condition = ""
    if funnel_dates:
        date_values = ", ".join(f"DATE '{value}'" for value in sorted(funnel_dates))
        date_condition = f" AND t.funnel_date IN ({date_values})"
    spark.sql(f"""
        MERGE INTO {target_table} t
        USING silver_funnel_batch s
          ON t.user_session = s.user_session AND t.product_id = s.product_id{date_condition}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def run_full(spark: SparkSession, args: argparse.Namespace) -> None:
    print(f"event_date 범위: {args.from_date or '(전체)'} ~ {args.to_date or '(전체)'}")
    print(f"소스 테이블: {args.source_table}  대상 테이블: {args.target_table}")

    events = read_events(spark, args.source_table, args.from_date, args.to_date)
    session_funnel = build_session_funnel(events)
    purchase_evidence = events.filter(F.col("event_type") == "purchase")
    funnel = attach_cross_session(session_funnel, purchase_evidence)

    stats = funnel.agg(
        F.count("*").alias("funnels"),
        F.sum("viewed").alias("viewed"),
        F.sum("carted").alias("carted"),
        F.sum("purchased").alias("purchased"),
        F.sum("converted_later").alias("converted_later"),
    ).collect()[0]

    merge_into_funnel(spark, funnel, args.target_table)

    print(f"퍼널={stats['funnels']} viewed={stats['viewed']} carted={stats['carted']}")
    print(f"세션 내 전환={stats['purchased']} 지연 전환={stats['converted_later']}")

    total = spark.table(args.target_table).count()
    print(f"{args.target_table} 전체 행 수={total}")


def read_batch_events(spark: SparkSession, path: str) -> DataFrame:
    # bronze_to_silver_events가 이번 실행에서 처리한 이벤트를 낸 산출물.
    # 빈 배치(Parquet 스키마 추론 실패 위험)는 Airflow가 event_count=0으로 감지해
    # 이 작업 자체를 건너뛰는 게 1차 방어선이고, 아래 빈 배치 체크는 2차 방어선이다.
    return spark.read.parquet(path).select(*BATCH_EVENT_COLUMNS)


def build_direct_keys(batch_events: DataFrame) -> DataFrame:
    return (
        batch_events.select("user_session", "product_id")
        .filter("user_session IS NOT NULL AND product_id IS NOT NULL")
        .distinct()
    )


def empty_funnel_keys(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], "user_session STRING, product_id STRING")


def build_propagated_key_partitions(batch_purchases: DataFrame) -> list:
    # 구매일별 30일 역방향 파티션의 합집합을 만든다.
    dates = [
        row["event_date"]
        for row in (
            batch_purchases.select("event_date")
            .where(F.col("event_date").isNotNull())
            .distinct()
            .collect()
        )
    ]
    if len(dates) > 366:
        raise ValueError("purchase event_date 범위가 비정상적으로 큽니다")
    return sorted(
        {d - timedelta(days=offset) for d in dates for offset in range(CROSS_SESSION_WINDOW_DAYS + 1)}
    )


def build_propagated_keys(spark: SparkSession, batch_purchases: DataFrame, target_table: str) -> DataFrame:
    partitions = build_propagated_key_partitions(batch_purchases)
    if not partitions:
        return empty_funnel_keys(spark)

    candidate_funnels = (
        spark.table(target_table)
        .filter(F.col("funnel_date").isin(partitions))
        .filter(F.col("purchased") == 0)
    )
    candidate_funnels.createOrReplaceTempView("propagation_candidates")
    batch_purchases.createOrReplaceTempView("batch_purchases_view")

    return spark.sql(f"""
        SELECT DISTINCT f.user_session, f.product_id
        FROM propagation_candidates f
        JOIN batch_purchases_view p
          ON  f.user_id      = p.user_id
          AND f.product_id   = p.product_id
          AND p.user_session <> f.user_session
          AND p.event_time  >  coalesce(f.first_cart_ts, f.first_view_ts)
          AND p.event_time  <= coalesce(f.first_cart_ts, f.first_view_ts)
                              + INTERVAL {CROSS_SESSION_WINDOW_DAYS} DAYS
    """)


def read_events_for_keys(spark: SparkSession, source_table: str, affected_keys: DataFrame) -> DataFrame:
    # 세션 전체 이력을 읽어 안전하게 재구성한다.
    return spark.table(source_table).join(affected_keys, ["user_session", "product_id"], "left_semi")


def empty_purchase_evidence(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [], "user_id STRING, product_id STRING, user_session STRING, event_time TIMESTAMP"
    )


def build_purchase_evidence_partitions(funnels: DataFrame) -> list:
    dates = [
        row["anchor_date"]
        for row in (
            funnels.filter(F.col("purchased") == 0)
            .select(F.to_date(F.coalesce("first_cart_ts", "first_view_ts")).alias("anchor_date"))
            .where(F.col("anchor_date").isNotNull())
            .distinct()
            .collect()
        )
    ]
    if len(dates) > 366:
        raise ValueError("funnel anchor_date 범위가 비정상적으로 큽니다")
    return sorted(
        {d + timedelta(days=offset) for d in dates for offset in range(CROSS_SESSION_WINDOW_DAYS + 1)}
    )


def read_purchase_evidence(spark: SparkSession, source_table: str, funnels: DataFrame) -> DataFrame:
    # 전체 Silver에서 anchor 기준 30일 구매 근거를 다시 조회한다.
    partitions = build_purchase_evidence_partitions(funnels)
    if not partitions:
        return empty_purchase_evidence(spark)

    candidate_funnels = (
        funnels.filter(F.col("purchased") == 0).select("user_id", "product_id").distinct()
    )

    return (
        spark.table(source_table)
        .filter(F.col("event_type") == "purchase")
        .filter(F.col("event_date").isin(partitions))
        .join(candidate_funnels, ["user_id", "product_id"], "left_semi")
        .select("user_id", "product_id", "user_session", "event_time")
    )


def run_incremental(spark: SparkSession, args: argparse.Namespace) -> dict:
    print(f"배치 입력: {args.batch_input_path}")
    print(f"소스 테이블: {args.source_table}  대상 테이블: {args.target_table}")

    batch_events = read_batch_events(spark, args.batch_input_path)
    batch_events.persist()

    if batch_events.limit(1).count() == 0:
        print("배치가 비어 있어 종료합니다")
        batch_events.unpersist()
        return {"affected_key_count": 0, "funnel_dates": []}

    batch_purchases = batch_events.filter(F.col("event_type") == "purchase")

    direct_keys = build_direct_keys(batch_events)
    propagated_keys = build_propagated_keys(spark, batch_purchases, args.target_table)
    affected_keys = direct_keys.union(propagated_keys).distinct()
    affected_keys.persist()

    if affected_keys.limit(1).count() == 0:
        print("영향받은 키가 없어 종료합니다")
        affected_keys.unpersist()
        batch_events.unpersist()
        return {"affected_key_count": 0, "funnel_dates": []}

    # MERGE 전에 기존 날짜를 확정한다.
    old_dates = {
        row["funnel_date"]
        for row in (
            spark.table(args.target_table)
            .join(affected_keys, ["user_session", "product_id"], "left_semi")
            .select("funnel_date")
            .distinct()
            .collect()
        )
    }

    events_for_keys = read_events_for_keys(spark, args.source_table, affected_keys)
    session_funnel = build_session_funnel(events_for_keys)
    purchase_evidence = read_purchase_evidence(spark, args.source_table, session_funnel)
    rebuilt_funnels = attach_cross_session(session_funnel, purchase_evidence)
    rebuilt_funnels.persist()

    stats = rebuilt_funnels.agg(
        F.count("*").alias("funnels"),
        F.sum("converted_later").alias("converted_later"),
    ).collect()[0]

    new_dates = {
        row["funnel_date"] for row in rebuilt_funnels.select("funnel_date").distinct().collect()
    }

    affected_funnel_dates = sorted(old_dates | new_dates)
    merge_into_funnel(spark, rebuilt_funnels, args.target_table, set(affected_funnel_dates))
    affected_key_count = affected_keys.count()
    print(f"영향받은 키={affected_key_count} 재계산된 funnel={stats['funnels']} 지연 전환={stats['converted_later']}")
    print(f"영향받은 funnel_date={[str(d) for d in affected_funnel_dates]}")

    total = spark.table(args.target_table).count()
    print(f"{args.target_table} 전체 행 수={total}")

    rebuilt_funnels.unpersist()
    affected_keys.unpersist()
    batch_events.unpersist()
    return {
        "affected_key_count": affected_key_count,
        "funnel_dates": [str(value) for value in affected_funnel_dates],
    }


def main() -> None:
    args = parse_args()
    spark = build_spark("silver_events_to_funnel", args.s3_bucket, args.aws_region)

    if args.mode == "full":
        run_full(spark, args)
        result = None
    else:
        result = run_incremental(spark, args)

    spark.stop()
    if result is not None:
        print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
