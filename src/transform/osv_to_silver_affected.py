
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


def json_string(value):
    """Serialize nested OSV data while preserving its structure."""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def main():
    project_root = Path(__file__).resolve().parents[2]

    raw_root = project_root / "data" / "raw" / "osv" / "vulnerabilities"
    output_path = project_root / "data" / "silver" / "osv" / "affected_packages"

    if not raw_root.exists():
        raise FileNotFoundError(f"Raw OSV directory not found: {raw_root}")

    raw_files = sorted(raw_root.rglob("*.json"))

    if not raw_files:
        raise FileNotFoundError(f"No JSON files found in {raw_root}")

    rows = []
    skipped_files = 0
    records_without_affected_entries = 0

    for file_path in raw_files:
        try:
            with file_path.open("r", encoding="utf-8") as file:
                envelope = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"Could not read {file_path}: {exc}")
            skipped_files += 1
            continue

        response = envelope.get("response")

        if (
            envelope.get("http_status") != 200
            or not isinstance(response, dict)
            or not response.get("id")
        ):
            skipped_files += 1
            continue

        affected_entries = response.get("affected") or []

        if not isinstance(affected_entries, list):
            print(f"Unexpected affected format in {file_path}")
            skipped_files += 1
            continue

        if not affected_entries:
            records_without_affected_entries += 1
            continue

        for affected in affected_entries:
            if not isinstance(affected, dict):
                continue

            package = affected.get("package") or {}

            if not isinstance(package, dict):
                package = {}

            rows.append(
                {
                    "vulnerability_id": response["id"],
                    "ecosystem": package.get("ecosystem"),
                    "package_name": package.get("name"),
                    "package_purl": package.get("purl"),
                    "affected_ranges_json": json_string(
                        affected.get("ranges")
                    ),
                    "affected_versions_json": json_string(
                        affected.get("versions")
                    ),
                    "ecosystem_specific_json": json_string(
                        affected.get("ecosystem_specific")
                    ),
                    "database_specific_json": json_string(
                        affected.get("database_specific")
                    ),
                    "source": envelope.get("source"),
                    "ingested_at": envelope.get("ingested_at"),
                    "run_id": envelope.get("run_id"),
                    "http_status": envelope.get("http_status"),
                }
            )

    if not rows:
        raise RuntimeError(
            "No affected-package entries found. "
            "Check the raw OSV records before proceeding."
        )

    schema = StructType(
        [
            StructField("vulnerability_id", StringType(), False),
            StructField("ecosystem", StringType(), True),
            StructField("package_name", StringType(), True),
            StructField("package_purl", StringType(), True),
            StructField("affected_ranges_json", StringType(), True),
            StructField("affected_versions_json", StringType(), True),
            StructField("ecosystem_specific_json", StringType(), True),
            StructField("database_specific_json", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("http_status", IntegerType(), True),
        ]
    )

    spark = (
        SparkSession.builder
        .appName("OSVToSilverAffectedPackages")
        .getOrCreate()
    )

    try:
        df = spark.createDataFrame(rows, schema=schema)

        print(f"Raw OSV files discovered: {len(raw_files)}")
        print(f"Files skipped due to errors: {skipped_files}")
        print(
            "Vulnerability records without affected entries: "
            f"{records_without_affected_entries}"
        )
        print(f"Affected-package rows created: {df.count()}")

        print("\n=== Sample affected-package entries ===")
        df.select(
            "vulnerability_id",
            "ecosystem",
            "package_name",
            "package_purl",
        ).show(20, truncate=False)

        print("\n=== Schema ===")
        df.printSchema()

        df.write.mode("overwrite").parquet(str(output_path))

        print(f"\nOutput written to: {output_path}")
        print("OSV affected-package transformation completed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
