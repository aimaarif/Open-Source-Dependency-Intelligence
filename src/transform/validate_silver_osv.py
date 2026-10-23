
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    project_root = Path(__file__).resolve().parents[2]
    silver_path = project_root / "data" / "silver" / "osv" / "vulnerabilities"

    if not silver_path.exists():
        raise FileNotFoundError(f"Silver OSV dataset not found: {silver_path}")

    spark = (
        SparkSession.builder
        .appName("ValidateSilverOSV")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(silver_path))

        total_rows = df.count()
        distinct_ids = df.select("vulnerability_id").distinct().count()

        print("\n=== Silver OSV Validation ===")
        print(f"Total vulnerability records: {total_rows}")
        print(f"Distinct vulnerability IDs: {distinct_ids}")

        # Required fields must not be null or blank.
        required_fields = [
            "vulnerability_id",
            "source",
            "ingested_at",
            "run_id",
            "http_status",
        ]

        print("\n=== Required-field checks ===")
        required_failures = 0

        for field in required_fields:
            condition = F.col(field).isNull()

            if field != "http_status":
                condition = condition | (F.trim(F.col(field)) == "")

            missing_count = df.filter(condition).count()
            required_failures += missing_count

            print(f"{field}: {missing_count} missing values")

        # A vulnerability ID should appear once per extraction run.
        duplicate_groups = (
            df.groupBy("vulnerability_id", "run_id")
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_group_count = duplicate_groups.count()
        print(f"\nDuplicate vulnerability/run groups: {duplicate_group_count}")

        if duplicate_group_count:
            duplicate_groups.show(20, truncate=False)

        # Check that the detail records came back successfully.
        non_200_count = df.filter(F.col("http_status") != 200).count()
        print(f"Records with non-200 HTTP status: {non_200_count}")

        # Nested fields are intentionally stored as JSON strings.
        missing_affected = df.filter(
            F.col("affected_json").isNull()
            | (F.trim(F.col("affected_json")) == "")
        ).count()

        missing_summary = df.filter(
            F.col("summary").isNull()
            | (F.trim(F.col("summary")) == "")
        ).count()

        print(f"Records with missing affected information: {missing_affected}")
        print(f"Records with missing summary: {missing_summary}")

        print("\n=== Sample records ===")
        df.select(
            "vulnerability_id",
            "summary",
            "published",
            "modified",
            "run_id",
            "http_status",
        ).show(10, truncate=False)

        # Missing affected data or summaries are reported, not automatically
        # treated as failures because OSV records can vary in their fields.
        passed = (
            total_rows > 0
            and required_failures == 0
            and duplicate_group_count == 0
            and non_200_count == 0
        )

        if passed:
            print("PASS: Essential Silver OSV validation checks passed.")
        else:
            raise RuntimeError(
                "FAIL: Silver OSV validation found an essential data-quality issue."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
