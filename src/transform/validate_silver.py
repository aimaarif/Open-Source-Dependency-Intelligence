
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER_PATH = "data/silver/pypi/packages"

spark = (
    SparkSession.builder
    .appName("Validate-PyPI-Silver")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Read the persisted Parquet dataset, not the original DataFrame.
    df = spark.read.parquet(SILVER_PATH)

    total_rows = df.count()
    distinct_packages = df.select("package_name").distinct().count()
    distinct_runs = df.select("run_id").distinct().count()
    distinct_dates = df.select("snapshot_date").distinct().count()

    print("\n=== SILVER DATA QUALITY REPORT ===")
    print(f"Total snapshot rows: {total_rows}")
    print(f"Distinct packages: {distinct_packages}")
    print(f"Distinct ingestion runs: {distinct_runs}")
    print(f"Distinct snapshot dates: {distinct_dates}")

    print("\n=== NULL CHECKS ===")

    required_columns = [
        "package_name",
        "version",
        "snapshot_date",
        "run_id",
    ]

    for column in required_columns:
        null_count = df.filter(
            F.col(column).isNull()
        ).count()
        print(f"{column}: {null_count} null values")

    print("\n=== DUPLICATE CHECK ===")

    duplicate_count = (
        df.groupBy("package_name", "run_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(f"Duplicate package/run combinations: {duplicate_count}")

    print("\n=== SNAPSHOTS PER DATE ===")

    (
        df.groupBy("snapshot_date")
        .agg(
            F.count("*").alias("rows"),
            F.countDistinct("package_name").alias("packages"),
        )
        .orderBy("snapshot_date")
        .show(truncate=False)
    )

    print("\n=== VALIDATION RESULT ===")

    if total_rows == 0:
        print("FAIL: Silver dataset is empty.")
    elif duplicate_count > 0:
        print("FAIL: Duplicate package/run combinations found.")
    elif any(
        df.filter(F.col(column).isNull()).limit(1).count() > 0
        for column in required_columns
    ):
        print("FAIL: Required columns contain null values.")
    else:
        print("PASS: Basic Silver data quality checks passed.")

finally:
    spark.stop()