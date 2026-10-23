
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def normalize_package_name(column):
    """Apply Python package-name normalization for matching."""
    return F.regexp_replace(
        F.lower(F.trim(column)),
        r"[-_.]+",
        "-",
    )


def main():
    project_root = Path(__file__).resolve().parents[2]

    health_path = project_root / "data" / "gold" / "pypi" / "package_health"
    affected_path = (
        project_root / "data" / "silver" / "osv" / "affected_packages"
    )
    vulnerability_path = (
        project_root / "data" / "silver" / "osv" / "vulnerabilities"
    )
    output_path = (
        project_root / "data" / "gold" / "pypi" / "package_vulnerabilities"
    )

    for path in (health_path, affected_path, vulnerability_path):
        if not path.exists():
            raise FileNotFoundError(f"Required dataset not found: {path}")

    spark = (
        SparkSession.builder
        .appName("MatchOSVToPackages")
        .getOrCreate()
    )

    try:
        health = spark.read.parquet(str(health_path))
        affected = spark.read.parquet(str(affected_path))
        vulnerabilities = spark.read.parquet(str(vulnerability_path))

        # Gold package_health contains one latest row per package.
        packages = health.select(
            "package_name",
            F.col("version").alias("observed_version"),
            "latest_snapshot_date",
            "snapshot_count",
            "observed_version_count",
        ).withColumn(
            "normalized_package_name",
            normalize_package_name(F.col("package_name")),
        )

        # Limit matching to the PyPI ecosystem.
        pypi_affected = (
            affected
            .filter(F.lower(F.trim(F.col("ecosystem"))) == "pypi")
            .withColumn(
                "normalized_package_name",
                normalize_package_name(F.col("package_name")),
            )
            .withColumnRenamed("package_name", "affected_package_name")
        )


        # Preserve the affected-version constraints for later evaluation.
        candidates = packages.join(
            pypi_affected,
            on="normalized_package_name",
            how="inner",
        )


        # Add advisory descriptions and publication metadata.
        vulnerability_details = vulnerabilities.select(
            "vulnerability_id",
            "summary",
            "published",
            "modified",
            "severity_json",
        )

        candidates = candidates.join(
            vulnerability_details,
            on="vulnerability_id",
            how="left",
        )

        result = candidates.select(
            F.col("package_name"),
            "observed_version",
            "latest_snapshot_date",
            "snapshot_count",
            "observed_version_count",
            "vulnerability_id",
            "summary",
            "published",
            "modified",
            "severity_json",
            "ecosystem",
            "affected_package_name",
            "package_purl",
            "affected_ranges_json",
            "affected_versions_json",
            "run_id",
        ).withColumn(
            "version_assessment",
            F.lit("not_evaluated"),
        )

        package_count = packages.count()
        affected_count = pypi_affected.count()
        candidate_count = result.count()
        matched_packages = result.select("package_name").distinct().count()
        matched_vulnerabilities = (
            result.select("vulnerability_id").distinct().count()
        )

        print("\n=== OSV Package Matching ===")
        print(f"Gold packages checked: {package_count}")
        print(f"PyPI affected-package entries checked: {affected_count}")
        print(f"Candidate match rows: {candidate_count}")
        print(f"Distinct matched packages: {matched_packages}")
        print(f"Distinct matched vulnerabilities: {matched_vulnerabilities}")

        print("\n=== Candidate matches ===")
        result.select(
            "package_name",
            "observed_version",
            "vulnerability_id",
            "summary",
            "affected_ranges_json",
            "affected_versions_json",
            "version_assessment",
        ).show(30, truncate=False)

        result.write.mode("overwrite").parquet(str(output_path))

        print(f"\nCandidate matches written to: {output_path}")
        print(
            "Matching completed. Version constraints have not yet "
            "been evaluated."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
