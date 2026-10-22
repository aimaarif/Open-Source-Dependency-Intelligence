
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    project_path = Path.cwd()

    silver_path = str(
        project_path / "data" / "silver" / "pypi" / "packages"
    )
    gold_path = str(
        project_path / "data" / "gold" / "pypi" / "package_health"
    )

    spark = (
        SparkSession.builder
        .appName("SilverToGoldPackageHealth")
        .getOrCreate()
    )

    try:
        # 1. Read the cleaned Silver snapshots.
        silver_df = spark.read.parquet(silver_path)

        
        # 2. Calculate one set of history metrics per package.
        package_metrics = (
            silver_df
            .groupBy("package_name")
            .agg(
                F.count("*").alias("snapshot_count"),
                F.countDistinct("version").alias(
                    "observed_version_count"
                ),
                F.min("snapshot_date").alias("first_seen_date"),
                F.max("snapshot_date").alias("latest_snapshot_date"),
            )
        )

        # 3. Attach the package-level metrics to every snapshot.
        history_df = silver_df.join(
            package_metrics,
            on="package_name",
            how="left",
        )

        # 4. Rank snapshots so the latest one comes first.
        latest_window = Window.partitionBy(
            "package_name"
        ).orderBy(
            F.col("snapshot_date").desc(),
            F.col("ingested_at").desc(),
        )

        history_df = history_df.withColumn(
            "row_number",
            F.row_number().over(latest_window),
        )


        # 6. Select fields useful for package-level analysis.
        gold_df = latest_df.select(
            "package_name",
            "version",
            "summary",
            "requires_python",
            "license",
            "home_page",
            "package_url",
            "project_urls",
            "source",
            "first_seen_date",
            "latest_snapshot_date",
            "snapshot_count",
            "observed_version_count",
            (
                F.col("observed_version_count") > 1
            ).alias("version_changed_across_snapshots"),
            F.when(
                F.col("requires_dist").isNull(), 0
            ).otherwise(
                F.size("requires_dist")
            ).alias("declared_dependency_count"),
            F.col("summary").isNotNull().alias("has_summary"),
            F.col("requires_python").isNotNull().alias(
                "has_python_requirement"
            ),
            F.col("project_urls").isNotNull().alias(
                "has_project_urls"
            ),
            "run_id",
            "http_status",
        )

        # 7. Write Gold as Parquet.
        # Re-running this job replaces this Gold dataset,
        # rather than appending duplicate rows.
        (
            gold_df.write
            .mode("overwrite")
            .parquet(gold_path)
        )

        print(f"Silver rows read: {silver_df.count()}")
        print(f"Gold package rows: {gold_df.count()}")
        print(f"Gold output: {gold_path}")

        gold_df.orderBy("package_name").show(
            10, truncate=False
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
