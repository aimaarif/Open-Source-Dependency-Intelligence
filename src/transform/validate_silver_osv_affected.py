
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    project_root = Path(__file__).resolve().parents[2]
    data_path = (
        project_root
        / "data"
        / "silver"
        / "osv"
        / "affected_packages"
    )

    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")

    spark = (
        SparkSession.builder
        .appName("ValidateSilverOSVAffected")
        .getOrCreate()
    )

    try:
        df = spark.read.parquet(str(data_path))

        total_rows = df.count()
        distinct_vulnerabilities = df.select(
            "vulnerability_id"
        ).distinct().count()

        print("\n=== Silver OSV Affected-Packages Validation ===")
        print(f"Total affected-package rows: {total_rows}")
        print(f"Distinct vulnerability IDs: {distinct_vulnerabilities}")

        required_fields = [
            "vulnerability_id",
            "package_name",
            "run_id",
        ]

        required_failures = 0

        print("\n=== Required-field checks ===")
        for field in required_fields:
            missing = df.filter(
                F.col(field).isNull() | (F.trim(F.col(field)) == "")
            ).count()

            required_failures += missing
            print(f"{field}: {missing} missing")

        non_200 = df.filter(
            F.col("http_status").isNull()
            | (F.col("http_status") != 200)
        ).count()

        print(f"\nNon-200 or missing HTTP status rows: {non_200}")

        missing_ecosystem = df.filter(
            F.col("ecosystem").isNull()
            | (F.trim(F.col("ecosystem")) == "")
        ).count()

        missing_range_and_versions = df.filter(
            F.col("affected_ranges_json").isNull()
            & F.col("affected_versions_json").isNull()
        ).count()

        print(f"Rows with missing ecosystem: {missing_ecosystem}")
        print(
            "Rows missing both range and explicit-version data: "
            f"{missing_range_and_versions}"
        )

        # Report repeated vulnerability/package combinations.
        # These are reported, not automatically treated as errors:
        # a vulnerability may have multiple affected entries.
        duplicate_groups = (
            df.groupBy(
                "vulnerability_id",
                "ecosystem",
                "package_name",
                "run_id",
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_count = duplicate_groups.count()
        print(
            "Repeated vulnerability/package/run groups: "
            f"{duplicate_count}"
        )

        if duplicate_count:
            duplicate_groups.show(20, truncate=False)

        print("\n=== Ecosystem distribution ===")
        (
            df.groupBy("ecosystem")
            .count()
            .orderBy(F.col("count").desc())
            .show(20, truncate=False)
        )

        print("\n=== Sample rows ===")
        df.select(
            "vulnerability_id",
            "ecosystem",
            "package_name",
            "package_purl",
            "affected_ranges_json",
            "affected_versions_json",
        ).show(10, truncate=False)

        passed = (
            total_rows > 0
            and required_failures == 0
            and non_200 == 0
        )

        if passed:
            print(
                "\nPASS: Essential affected-package validation checks passed."
            )
        else:
            raise RuntimeError(
                "FAIL: Essential affected-package validation failed."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
