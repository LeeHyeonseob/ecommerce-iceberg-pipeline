import argparse
import os
from pathlib import Path

from dotenv import load_dotenv
from spark_session import build_spark

load_dotenv()

DDL_DIR = Path(__file__).resolve().parents[1] / "ddl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--s3-bucket", default=os.environ.get("S3_BUCKET"))
    parser.add_argument("--aws-region", default=os.environ.get("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--files", default="*.sql")
    return parser.parse_args()


def split_statements(sql: str) -> list[str]:
    # 주석 줄을 제거한 뒤 세미콜론으로 분리 (문자열 리터럴 안의 세미콜론은 쓰지 않음)
    lines = [ln for ln in sql.splitlines() if not ln.strip().startswith("--")]
    return [s.strip() for s in "\n".join(lines).split(";") if s.strip()]


def main() -> None:
    args = parse_args()
    spark = build_spark("run_ddl", args.s3_bucket, args.aws_region)

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
