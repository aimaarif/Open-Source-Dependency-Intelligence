
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_PATH = str(
    PROJECT_ROOT / "data" / "silver" / "pypi" / "packages"
)

GOLD_PATH = str(
    PROJECT_ROOT / "data" / "gold" / "pypi" / "package_dependencies"
)


def main():
    spark = (
        SparkSession.builder
        .appName("ValidateGoldPackageDependencies")
        .getOrCreate()
    )

    try:
        # 1. Read the existing Silver and Gold datasets.
        silver_df = spark.read.parquet(SILVER_PATH)
        gold_df = spark.read.parquet(GOLD_PATH)

        # 2. Find the latest Silver snapshot for each package.
        latest_window = (
            Window.partitionBy("package_name")
            .orderBy(
                F.col("snapshot_date").desc(),
                F.col("ingested_at").desc(),
            )
        )

        latest_df = (
            silver_df
            .withColumn(
                "row_number",
                F.row_number().over(latest_window),
            )
            .filter(F.col("row_number") == 1)
            .drop("row_number")
        )

        # 3. Calculate the expected dependency declarations.
        expected_df = (
            latest_df
            .select(
                "package_name",
                "version",
                "snapshot_date",
                "run_id",
                F.explode("requires_dist").alias(
                    "dependency_declaration"
                ),
            )
            .filter(
                F.col("dependency_declaration").isNotNull()
                & (
                    F.trim("dependency_declaration") != ""
                )
            )
        )

        expected_count = expected_df.count()
        gold_count = gold_df.count()

        expected_packages = (
            expected_df.select("package_name").distinct().count()
        )
        gold_packages = (
            gold_df.select("package_name").distinct().count()
        )

        # 4. Check required fields for missing or blank values.
        required_columns = [
            "package_name",
            "version",
            "snapshot_date",
            "run_id",
            "dependency_name",
            "normalized_dependency_name",
            "dependency_declaration",
        ]

        missing_condition = None

        for column_name in required_columns:
            condition = (
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name).cast("string")) == "")
            )

            if missing_condition is None:
                missing_condition = condition
            else:
                missing_condition = missing_condition | condition

        missing_rows = gold_df.filter(missing_condition).count()

        # 5. Check whether dependency names were extracted.
        invalid_names = gold_df.filter(
            F.col("dependency_name").isNull()
            | (F.trim("dependency_name") == "")
            | F.col("normalized_dependency_name").isNull()
            | (F.trim("normalized_dependency_name") == "")
        ).count()

        # 6. Find repeated declarations within the same snapshot.
        duplicate_groups = (
            gold_df
            .groupBy(
                "package_name",
                "version",
                "snapshot_date",
                "run_id",
                "dependency_declaration",
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        # 7. Check whether any expected declarations are absent.
        gold_keys = gold_df.select(
            "package_name",
            "version",
            "snapshot_date",
            "run_id",
            "dependency_declaration",
        ).distinct()

        missing_declarations = (
            expected_df
            .select(
                "package_name",
                "version",
                "snapshot_date",
                "run_id",
                "dependency_declaration",
            )
            .distinct()
            .join(
                gold_keys,
                on=[
                    "package_name",
                    "version",
                    "snapshot_date",
                    "run_id",
                    "dependency_declaration",
                ],
                how="left_anti",
            )
            .count()
        )

        
        # Inspect repeated dependency declarations.
        duplicate_details = (
            gold_df
            .groupBy(
                "package_name",
                "version",
                "snapshot_date",
                "dependency_declaration",
            )
            .agg(
                F.count("*").alias("occurrences")
            )
            .filter(F.col("occurrences") > 1)
            .orderBy(
                F.col("occurrences").desc(),
                "package_name",
            )
        )

        print("\n--- Repeated Dependency Declarations ---")
        duplicate_details.show(50, truncate=False)


        # 8. Print validation results.
        print("\n--- Gold Dependency Validation ---")
        print("Expected declarations:", expected_count)
        print("Gold dependency rows:", gold_count)
        print("Expected packages with dependencies:", expected_packages)
        print("Gold packages with dependencies:", gold_packages)
        print("Rows with missing required values:", missing_rows)
        print("Rows with invalid extracted names:", invalid_names)
        print("Missing source declarations:", missing_declarations)
        print("Duplicate declaration groups:", duplicate_groups)

        # 9. Decide whether the essential checks passed.
        passed = (
            expected_count == gold_count
            and expected_packages == gold_packages
            and missing_rows == 0
            and invalid_names == 0
            and missing_declarations == 0
        )

        if passed:
            print("\nPASS: Essential dependency validation checks passed.")
        else:
            raise RuntimeError(
                "FAIL: Dependency validation found a mismatch."
            )

        if duplicate_groups > 0:
            print(
                "WARNING: Repeated declarations exist. "
                "Review them before deduplicating; "
                "they have not been removed."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
