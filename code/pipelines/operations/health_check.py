import argparse
import json
import os
from pathlib import Path

from pipelines.common.spark_session import build_spark

# 상세 쿼리는 파일명이 아니라 detail/ 디렉터리로 구분한다.
DETAIL_DIR_NAME = "detail"
DEFAULT_DETAIL_PARTITIONS = 14
# detail/*.sql의 UNION 분기에 있는 table_name 리터럴과 일치해야 한다.
# 허용 목록으로 검증해야 오타가 0행이 아니라 오류로 드러난다.
DETAIL_TABLES = (
    "silver_events",
    "silver_funnel",
    "gold_daily_gmv",
    "gold_funnel_daily",
    "gold_category_gmv",
    "gold_pipeline_sla",
    "gold_data_quality",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"), required=os.environ.get("S3_BUCKET") is None)
    parser.add_argument("--aws-region", default="ap-northeast-2")
    parser.add_argument(
        "--detail",
        action="store_true",
        help="파티션별 상세 파일 상태 쿼리까지 실행",
    )
    parser.add_argument(
        "--detail-tables",
        default="",
        help=f"상세 쿼리 대상 테이블(콤마 구분). 비우면 전체. 가능: {', '.join(DETAIL_TABLES)}",
    )
    parser.add_argument(
        "--detail-partitions",
        type=int,
        default=DEFAULT_DETAIL_PARTITIONS,
        help=f"상세 쿼리에서 테이블별로 조회할 최근 파티션 수 (기본 {DEFAULT_DETAIL_PARTITIONS})",
    )
    args = parser.parse_args()
    args.detail_tables = [name.strip() for name in args.detail_tables.split(",") if name.strip()]
    unknown = [name for name in args.detail_tables if name not in DETAIL_TABLES]
    if unknown:
        parser.error(
            f"--detail-tables에 없는 테이블입니다: {', '.join(unknown)} "
            f"(가능: {', '.join(DETAIL_TABLES)})"
        )
    if args.detail_partitions < 1:
        parser.error("--detail-partitions는 1 이상이어야 합니다")
    return args


def is_detail_query(path: Path, query_dir: Path) -> bool:
    return DETAIL_DIR_NAME in path.relative_to(query_dir).parts


def query_paths(query_dir: Path, detail: bool) -> list[Path]:
    paths = sorted(query_dir.rglob("[0-9][0-9]_*.sql"))
    if detail:
        return paths
    return [path for path in paths if not is_detail_query(path, query_dir)]


def render_detail_query(sql: str, tables: list[str], partition_limit: int) -> str:
    # 테이블 필터는 UNION ALL 각 분기의 리터럴과 상수 폴딩돼 대상 외 테이블의 metadata 스캔을 건너뛴다.
    table_filter = (
        "TRUE" if not tables
        else "table_name IN ({})".format(", ".join(f"'{name}'" for name in tables))
    )
    return sql.replace("__TABLE_FILTER__", table_filter).replace(
        "__PARTITION_LIMIT__", str(partition_limit)
    )


def main() -> None:
    args = parse_args()
    spark = build_spark("health_check", args.s3_bucket, args.aws_region, log_level="WARN")
    query_dir = Path(__file__).parents[2] / "health-queries"
    results = {}
    try:
        for path in query_paths(query_dir, args.detail):
            query_name = str(path.relative_to(query_dir))
            print(f"=== {query_name} ===")
            sql = path.read_text()
            if is_detail_query(path, query_dir):
                sql = render_detail_query(sql, args.detail_tables, args.detail_partitions)
            rows = spark.sql(sql).collect()
            results[query_name] = [row.asDict() for row in rows]
            for row in results[query_name]:
                print(row)
    finally:
        spark.stop()
    print(json.dumps({"query_count": len(results), "queries": list(results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
