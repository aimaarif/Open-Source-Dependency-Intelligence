
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = (
    SparkSession.builder
    .appName("BuildPackageRisk")
    .getOrCreate()
)

# Input datasets
package_health_path = (
    "/opt/project/data/gold/pypi/package_health"
)

assessment_path = (
    "/opt/project/data/gold/pypi/package_vulnerability_assessment"
)

# Output dataset
output_path = (
    "/opt/project/data/gold/pypi/package_risk"
)

# Read the package inventory and version assessments.
packages = spark.read.parquet(package_health_path)
assessments = spark.read.parquet(assessment_path)

# Count assessments by package.
# Each assessment row represents a package/version/advisory result.
risk_summary = (
    assessments
    .groupBy("package_name")
    .agg(
        F.countDistinct("vulnerability_id")
        .alias("assessed_vulnerability_count"),

        F.countDistinct(
            F.when(
                F.col("version_assessment") == "affected",
                F.col("vulnerability_id")
            )
        ).alias("affected_vulnerability_count"),

        F.countDistinct(
            F.when(
                F.col("version_assessment") == "unknown",
                F.col("vulnerability_id")
            )
        ).alias("unknown_assessment_count"),

        F.countDistinct(
            F.when(
                F.col("version_assessment") == "not_affected",
                F.col("vulnerability_id")
            )
        ).alias("not_affected_count"),
    )
)

# Retain packages even when no OSV candidate matched them.
result = (
    packages
    .join(risk_summary, on="package_name", how="left")
    .fillna(
        0,
        subset=[
            "assessed_vulnerability_count",
            "affected_vulnerability_count",
            "unknown_assessment_count",
            "not_affected_count",
        ],
    )
)

# Use cautious labels; this is not a complete security score.
result = result.withColumn(
    "risk_status",
    F.when(
        F.col("affected_vulnerability_count") > 0,
        F.lit("affected")
    )
    .when(
        F.col("unknown_assessment_count") > 0,
        F.lit("needs_review")
    )
    .when(
        F.col("assessed_vulnerability_count") > 0,
        F.lit("no_affected_version_match")
    )
    .otherwise(F.lit("no_known_osv_match"))
)

# Write the Gold dataset.
(
    result.write
    .mode("overwrite")
    .parquet(output_path)
)

print("Package risk dataset created.")
print(f"Output: {output_path}")
print(f"Package count: {result.count()}")

print("Risk status distribution:")
result.groupBy("risk_status").count().show()

print("Sample package risk records:")
result.show(20, truncate=False)

spark.stop()
