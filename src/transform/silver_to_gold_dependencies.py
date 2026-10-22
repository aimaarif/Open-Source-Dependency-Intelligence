
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
        .appName("SilverToGoldPackageDependencies")
        .getOrCreate()
    )

    try:
        # 1. Read the existing Silver package snapshots.
        silver_df = spark.read.parquet(SILVER_PATH)

        # 2. Select the latest snapshot for each package.
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

        # 3. Expand the dependency array into individual rows.
        dependencies_df = (
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
                & (F.trim("dependency_declaration") != "")
            )
        )

        # 4. Extract the declared dependency's package name.
        dependencies_df = dependencies_df.withColumn(
            "dependency_name",
            F.regexp_extract(
                "dependency_declaration",
                r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)",
                1,
            ),
        )

        # 5. Normalize the name for easier matching.
        # For example: Some_Package -> some-package
        dependencies_df = dependencies_df.withColumn(
            "normalized_dependency_name",
            F.regexp_replace(
                F.lower("dependency_name"),
                r"[-_.]+",
                "-",
            ),
        )

        # 6. Extract optional extras, such as [security].
        dependencies_df = dependencies_df.withColumn(
            "dependency_extras",
            F.regexp_extract(
                "dependency_declaration",
                r"\[([^\]]*)\]",
                1,
            ),
        )

        # 7. Preserve the requirement after the package name.
        # This may contain version constraints, extras, or a URL.
        dependencies_df = dependencies_df.withColumn(
            "dependency_requirement",
            F.trim(
                F.regexp_replace(
                    F.regexp_replace(
                        "dependency_declaration",
                        r"^\s*[A-Za-z0-9][A-Za-z0-9._-]*",
                        "",
                    ),
                    r"\s*;\s*.*$",
                    "",
                )
            ),
        )

        # 8. Extract an optional environment marker.
        dependencies_df = dependencies_df.withColumn(
            "environment_marker",
            F.regexp_extract(
                "dependency_declaration",
                r";\s*(.+)$",
                1,
            ),
        )

        # 9. Remove declarations whose package name could not be parsed.
        dependencies_df = dependencies_df.filter(
            F.col("dependency_name") != ""
        )

        # 10. Select the final Gold columns.
        gold_df = dependencies_df.select(
            "package_name",
            "version",
            "snapshot_date",
            "run_id",
            "dependency_name",
            "normalized_dependency_name",
            "dependency_extras",
            "dependency_requirement",
            "environment_marker",
            "dependency_declaration",
        )

        # 11. Write the Gold dataset.
        gold_df.write.mode("overwrite").parquet(GOLD_PATH)

        # 12. Report basic results.
        print("\nGold dependency rows:", gold_df.count())
        print(
            "Packages with dependencies:",
            gold_df.select("package_name").distinct().count(),
        )
        print("Gold output:", GOLD_PATH)

        print("\nSample dependency rows:")
        gold_df.orderBy(
            "package_name",
            "normalized_dependency_name",
        ).show(20, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
