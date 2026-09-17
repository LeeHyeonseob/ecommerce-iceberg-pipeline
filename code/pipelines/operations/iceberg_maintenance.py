import argparse
import json
import os
import re
import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import Callable

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pipelines.common.spark_session import build_spark

load_dotenv()

TABLES = [
    "glue.ecommerce_lakehouse.silver_events",
    "glue.ecommerce_lakehouse.silver_funnel",
    "glue.ecommerce_lakehouse.gold_daily_gmv",
    "glue.ecommerce_lakehouse.gold_funnel_daily",
    "glue.ecommerce_lakehouse.gold_category_gmv",
    "glue.ecommerce_lakehouse.gold_pipeline_sla",
    "glue.ecommerce_lakehouse.gold_data_quality",
]
MOR_TABLES = {
    "glue.ecommerce_lakehouse.silver_events",
    "glue.ecommerce_lakehouse.silver_funnel",
}
SNAPSHOT_RETENTION_DAYS = 30

# 실행 순서는 입력 순서가 아니라 이 정의 순서로 고정한다. data file 재작성이 만든
# dangling delete를 다음 단계가 정리하고, 삭제는 metadata를 먼저 줄인 뒤 파일시스템과 대조한다.
STEP_ORDER = (
    "rewrite_data_files",
    "rewrite_position_delete_files",
    "rewrite_manifests",
    "expire_snapshots",
    "remove_orphan_files",
)
# 재작성과 삭제는 되돌림 가능성이 달라 별도 DAG로 실행한다(docs/decisions/003).
REWRITE_STEPS = STEP_ORDER[:3]
CLEANUP_STEPS = STEP_ORDER[3:]
STEP_GROUPS = {
    "rewrite": REWRITE_STEPS,
    "cleanup": CLEANUP_STEPS,
    "all": STEP_ORDER,
}
# COW 테이블에는 position delete가 생기지 않으므로 이 단계를 건너뛴다.
MOR_ONLY_STEPS = {"rewrite_position_delete_files"}

# procedure 옵션은 SQL 문자열로 들어가므로 문자 집합을 제한한다.
OPTION_TOKEN = re.compile(r"^[A-Za-z0-9._\-]+$")


def parse_steps(raw: str, parser: argparse.ArgumentParser) -> list[str]:
    tokens = [token.strip() for token in raw.split(",") if token.strip()]
    if not tokens:
        parser.error("--steps가 비어 있습니다")
    selected = set()
    allowed = ", ".join(list(STEP_GROUPS) + list(STEP_ORDER))
    for token in tokens:
        if token in STEP_GROUPS:
            selected.update(STEP_GROUPS[token])
        elif token in STEP_ORDER:
            selected.add(token)
        else:
            parser.error(f"--steps에 없는 단계입니다: {token} (가능: {allowed})")
    return [step for step in STEP_ORDER if step in selected]


def parse_options(pairs: list[str], flag: str, parser: argparse.ArgumentParser) -> dict:
    options = {}
    for pair in pairs:
        if "=" not in pair:
            parser.error(f"{flag}는 KEY=VALUE 형식이어야 합니다: {pair}")
        key, value = (part.strip() for part in pair.split("=", 1))
        for token in (key, value):
            if not OPTION_TOKEN.match(token):
                parser.error(f"{flag}에 쓸 수 없는 문자가 있습니다: {pair}")
        options[key] = value
    return options


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"), required=os.environ.get("S3_BUCKET") is None)
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--tables", default=",".join(TABLES))
    # 기본값을 두지 않는다. 빼먹으면 재작성만 의도했는데 삭제까지 실행된다.
    parser.add_argument(
        "--steps",
        required=True,
        help="실행할 단계(콤마 구분). 그룹: " + ", ".join(STEP_GROUPS) + " / 개별: " + ", ".join(STEP_ORDER),
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=None,
        help=f"삭제 단계의 보존 기간. 삭제 단계가 있으면 기본 {SNAPSHOT_RETENTION_DAYS}",
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="삭제 기준 시각(UTC, 'YYYY-MM-DD HH:MM:SS'). 생략하면 실행 시각. "
             "재시도마다 기준이 넓어지지 않도록 고정한다",
    )
    parser.add_argument(
        "--rewrite-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="rewrite_data_files에 전달할 옵션(반복 가능)",
    )
    parser.add_argument(
        "--delete-rewrite-option",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="rewrite_position_delete_files에 전달할 옵션(반복 가능)",
    )
    args = parser.parse_args()

    args.tables = [table.strip() for table in args.tables.split(",") if table.strip()]
    unknown = [table for table in args.tables if table not in TABLES]
    if unknown:
        parser.error(f"--tables에 없는 테이블입니다: {', '.join(unknown)}")

    args.steps = parse_steps(args.steps, parser)
    has_cleanup = any(step in CLEANUP_STEPS for step in args.steps)

    if args.retention_days is None:
        args.retention_days = SNAPSHOT_RETENTION_DAYS if has_cleanup else None
    elif not has_cleanup:
        parser.error("--retention-days는 삭제 단계(expire_snapshots, remove_orphan_files)가 있을 때만 유효합니다")
    elif args.retention_days < 1:
        parser.error("--retention-days는 1 이상이어야 합니다")

    if args.as_of is not None and not has_cleanup:
        parser.error("--as-of는 삭제 단계가 있을 때만 유효합니다")
    if args.as_of:
        try:
            args.as_of = datetime.strptime(args.as_of, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except ValueError:
            parser.error("--as-of 형식은 'YYYY-MM-DD HH:MM:SS'(UTC)입니다")
    else:
        args.as_of = datetime.now(timezone.utc)

    args.rewrite_options = parse_options(args.rewrite_option, "--rewrite-option", parser)
    args.delete_rewrite_options = parse_options(
        args.delete_rewrite_option, "--delete-rewrite-option", parser
    )
    for flag, options, step in (
        ("--rewrite-option", args.rewrite_options, "rewrite_data_files"),
        ("--delete-rewrite-option", args.delete_rewrite_options, "rewrite_position_delete_files"),
    ):
        if options and step not in args.steps:
            parser.error(f"{flag}을 주려면 --steps에 {step}이 포함돼야 합니다")
    return args


def procedure_table_name(table: str) -> str:
    return table.removeprefix("glue.")


def options_clause(options: dict) -> str:
    if not options:
        return ""
    pairs = ", ".join(f"'{key}', '{value}'" for key, value in options.items())
    return f", options => map({pairs})"


def run_timed(operation: Callable[[], object]) -> dict:
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    result = operation()
    return {
        "started_at": started_at.isoformat(),
        "duration_sec": round(time.monotonic() - started, 3),
        "result": result,
    }


def rewrite_position_delete_files(spark: SparkSession, table: str, options: dict) -> dict:
    row = spark.sql(
        f"CALL glue.system.rewrite_position_delete_files("
        f"table => '{procedure_table_name(table)}'{options_clause(options)})"
    ).collect()[0]
    return row.asDict()


def rewrite_data_files(spark: SparkSession, table: str, options: dict) -> dict:
    row = spark.sql(
        f"CALL glue.system.rewrite_data_files("
        f"table => '{procedure_table_name(table)}'{options_clause(options)})"
    ).collect()[0]
    return row.asDict()


def rewrite_manifests(spark: SparkSession, table: str) -> dict:
    row = spark.sql(
        f"CALL glue.system.rewrite_manifests(table => '{procedure_table_name(table)}')"
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


def step_runner(spark: SparkSession, table: str, args: argparse.Namespace, older_than: str):
    return {
        "rewrite_data_files": lambda: rewrite_data_files(spark, table, args.rewrite_options),
        "rewrite_position_delete_files": lambda: rewrite_position_delete_files(
            spark, table, args.delete_rewrite_options
        ),
        "rewrite_manifests": lambda: rewrite_manifests(spark, table),
        "expire_snapshots": lambda: expire_snapshots(spark, table, older_than),
        "remove_orphan_files": lambda: remove_orphan_files(spark, table, older_than),
    }


def maintain_table(spark: SparkSession, table: str, args: argparse.Namespace, older_than: str) -> dict:
    runners = step_runner(spark, table, args, older_than)
    table_result = {}
    for step in args.steps:
        if step in MOR_ONLY_STEPS and table not in MOR_TABLES:
            continue
        table_result[step] = run_timed(runners[step])
    return table_result


def main() -> None:
    args = parse_args()
    older_than = (
        args.as_of - timedelta(days=args.retention_days)
    ).strftime("%Y-%m-%d %H:%M:%S") if args.retention_days else None

    print(f"단계: {', '.join(args.steps)}")
    print(f"대상: {', '.join(args.tables)}")
    if older_than:
        print(f"보관 기준: {args.retention_days}일 "
              f"(as_of={args.as_of.strftime('%Y-%m-%d %H:%M:%S')}, older_than={older_than})")

    spark = build_spark("iceberg_maintenance", args.s3_bucket, args.aws_region, log_level="WARN")
    summary = {
        "steps": args.steps,
        "as_of": args.as_of.isoformat(),
        "retention_days": args.retention_days,
        "rewrite_options": args.rewrite_options,
        "delete_rewrite_options": args.delete_rewrite_options,
        "tables": {},
        "failed_tables": [],
    }
    try:
        for table in args.tables:
            print(f"=== {table} ===")
            try:
                summary["tables"][table] = maintain_table(spark, table, args, older_than)
            except Exception as exc:  # 한 테이블 실패가 나머지 테이블을 막지 않게 한다
                summary["tables"][table] = {"error": f"{type(exc).__name__}: {exc}"}
                summary["failed_tables"].append(table)
                traceback.print_exc()
            print(json.dumps(summary["tables"][table], ensure_ascii=False, default=str))
    finally:
        spark.stop()

    # Airflow가 파싱하는 마지막 출력 줄은 항상 이 JSON이어야 한다.
    print(json.dumps(summary, ensure_ascii=False, default=str))
    if summary["failed_tables"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
