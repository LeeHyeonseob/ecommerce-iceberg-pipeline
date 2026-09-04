import argparse
import json
import os
from pathlib import Path

from iceberg_maintenance import build_spark


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"), required=os.environ.get("S3_BUCKET") is None)
    parser.add_argument("--aws-region", default="ap-northeast-2")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark(args.s3_bucket, args.aws_region)
    query_dir = Path(__file__).parents[1] / "health-queries"
    results = {}
    try:
        for path in sorted(query_dir.rglob("[0-9][0-9]_*.sql")):
            print(f"=== {path.name} ===")
            rows = spark.sql(path.read_text()).collect()
            query_name = str(path.relative_to(query_dir))
            results[query_name] = [row.asDict() for row in rows]
            for row in results[query_name]:
                print(row)
    finally:
        spark.stop()
    print(json.dumps({"query_count": len(results), "queries": list(results)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
