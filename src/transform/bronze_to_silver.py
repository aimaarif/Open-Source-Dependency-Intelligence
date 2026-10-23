
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
)


# Project root: derived from this script's location.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_PATH = PROJECT_ROOT / "data" / "raw" / "pypi"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "pypi" / "packages"


# Read only the fields required for our first Silver dataset.
# Extra fields in the raw JSON, such as releases, are ignored.
schema = StructType([
    StructField("source", StringType(), True),
    StructField("package_name", StringType(), True),
    StructField("ingested_at", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("request_url", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("error_message", StringType(), True),
    StructField(
        "payload",
        StructType([
            StructField(
                "info",
                StructType([
                    StructField("name", StringType(), True),
                    StructField("version", StringType(), True),
                    StructField("summary", StringType(), True),
                    StructField("requires_python", StringType(), True),
                    StructField("license", StringType(), True),
                    StructField("home_page", StringType(), True),
                    StructField("package_url", StringType(), True),
                    StructField("author", StringType(), True),
                    StructField("author_email", StringType(), True),
                    StructField("maintainer", StringType(), True),
                    StructField(
                        "project_urls",
                        MapType(StringType(), StringType()),
                        True,
                    ),
                    StructField(
                        "requires_dist",
                        ArrayType(StringType()),
                        True,
                    ),
                ]),
                True,
            )
        ]),
        True,
    ),
])


def main():
    if not BRONZE_PATH.exists():
        raise FileNotFoundError(
            f"Bronze directory not found: {BRONZE_PATH}"
        )

    spark = (
        SparkSession.builder
        .appName("PyPI-Bronze-to-Silver")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(f"Reading Bronze data from: {BRONZE_PATH}")

        # Read existing JSON files recursively.
        # Explicit schema avoids inferring every nested release field.
        bronze_df = (
            spark.read
            .schema(schema)
            .option("recursiveFileLookup", "true")
            .option("multiLine", "true")
            .json(str(BRONZE_PATH))
        )

        input_count = bronze_df.count()

        successful_df = bronze_df.filter(
            (F.col("http_status") == 200)
            & F.col("payload.info.name").isNotNull()
        )

        failed_df = bronze_df.filter(
            (F.col("http_status") != 200)
            | F.col("http_status").isNull()
            | F.col("payload.info.name").isNull()
        )

        failed_count = failed_df.count()

        # Select and flatten the fields needed by the packages dataset.
        packages_df = successful_df.select(
            F.lower(
                F.regexp_replace(
                    F.trim(F.coalesce(
                        F.col("payload.info.name"),
                        F.col("package_name"),
                    )),
                    r"[-_.]+",
                    "-",
                )
            ).alias("package_name"),
            F.col("payload.info.version").alias("version"),
            F.col("payload.info.summary").alias("summary"),
            F.col("payload.info.requires_python").alias("requires_python"),
            F.col("payload.info.license").alias("license"),
            F.col("payload.info.home_page").alias("home_page"),
            F.col("payload.info.package_url").alias("package_url"),
            F.col("payload.info.author").alias("author"),
            F.col("payload.info.author_email").alias("author_email"),
            F.col("payload.info.maintainer").alias("maintainer"),
            F.col("payload.info.project_urls").alias("project_urls"),
            F.col("payload.info.requires_dist").alias("requires_dist"),
            F.col("source"),
            F.col("ingested_at"),
            F.to_date(
                F.to_timestamp("ingested_at")
            ).alias("snapshot_date"),
            F.col("run_id"),
            F.col("http_status"),
        )

        # One row per normalized package per ingestion run.
        # Keep the latest ingestion timestamp if a run contains duplicates.
        packages_df = (
            packages_df
            .withColumn(
                "_row_number",
                F.row_number().over(
                    __import__(
                        "pyspark.sql.window",
                        fromlist=["Window"],
                    ).Window.partitionBy(
                        "package_name", "run_id"
                    ).orderBy(F.col("ingested_at").desc())
                ),
            )
            .filter(F.col("_row_number") == 1)
            .drop("_row_number")
        )

        silver_count = packages_df.count()

        print(f"Bronze records read: {input_count}")
        print(f"Records without usable package metadata: {failed_count}")
        print(f"Silver package snapshot rows: {silver_count}")

        print("\nSilver schema:")
        packages_df.printSchema()

        print("\nSample rows:")
        packages_df.select(
            "package_name",
            "version",
            "requires_python",
            "snapshot_date",
            "run_id",
        ).show(10, truncate=False)

        # Parquet is columnar storage, useful for downstream analytics.
        # Re-running this script replaces only this Silver output directory.
        (
            packages_df.write
            .mode("overwrite")
            .parquet(str(SILVER_PATH))
        )

        print(f"Silver dataset written to: {SILVER_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
