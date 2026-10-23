
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
    """Preserve nested JSON fields without losing their structure."""
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False)


def main():
    project_root = Path(__file__).resolve().parents[2]
    raw_root = project_root / "data" / "raw" / "osv" / "vulnerabilities"
    output_path = project_root / "data" / "silver" / "osv" / "vulnerabilities"

    if not raw_root.exists():
        raise FileNotFoundError(f"OSV raw data directory not found: {raw_root}")

    raw_files = sorted(raw_root.rglob("*.json"))

    if not raw_files:
        raise FileNotFoundError(f"No OSV vulnerability JSON files found in {raw_root}")

    rows = []
    failed_records = []

    for file_path in raw_files:
        try:
            with file_path.open("r", encoding="utf-8") as file:
                envelope = json.load(file)
        except (OSError, json.JSONDecodeError) as exc:
            failed_records.append(f"{file_path}: {exc}")
            continue

        response = envelope.get("response")
        status = envelope.get("http_status")

        # Only successful, complete vulnerability records belong in this table.
        if status != 200 or not isinstance(response, dict) or not response.get("id"):
            failed_records.append(
                f"{file_path}: HTTP status={status}, "
                f"vulnerability_id={envelope.get('vulnerability_id')}"
            )
            continue

        rows.append(
            {
                "vulnerability_id": response.get("id"),
                "summary": response.get("summary"),
                "details": response.get("details"),
                "published": response.get("published"),
                "modified": response.get("modified"),
                "withdrawn": response.get("withdrawn"),
                "aliases_json": json_string(response.get("aliases")),
                "references_json": json_string(response.get("references")),
                "affected_json": json_string(response.get("affected")),
                "severity_json": json_string(response.get("severity")),
                "database_specific_json": json_string(
                    response.get("database_specific")
                ),
                "matched_packages_json": json_string(
                    envelope.get("matched_packages")
                ),
                "source": envelope.get("source"),
                "ingested_at": envelope.get("ingested_at"),
                "run_id": envelope.get("run_id"),
                "http_status": status,
                "error_message": envelope.get("error_message"),
            }
        )

    if failed_records:
        print(f"Records skipped because they were invalid or unsuccessful: {len(failed_records)}")
        for failure in failed_records[:10]:
            print(f"  - {failure}")

    if not rows:
        raise RuntimeError("No successful OSV vulnerability records were available.")

    schema = StructType(
        [
            StructField("vulnerability_id", StringType(), False),
            StructField("summary", StringType(), True),
            StructField("details", StringType(), True),
            StructField("published", StringType(), True),
            StructField("modified", StringType(), True),
            StructField("withdrawn", StringType(), True),
            StructField("aliases_json", StringType(), True),
            StructField("references_json", StringType(), True),
            StructField("affected_json", StringType(), True),
            StructField("severity_json", StringType(), True),
            StructField("database_specific_json", StringType(), True),
            StructField("matched_packages_json", StringType(), True),
            StructField("source", StringType(), True),
            StructField("ingested_at", StringType(), True),
            StructField("run_id", StringType(), True),
            StructField("http_status", IntegerType(), True),
            StructField("error_message", StringType(), True),
        ]
    )

    spark = (
        SparkSession.builder
        .appName("OSVToSilver")
        .getOrCreate()
    )

    try:
        df = spark.createDataFrame(rows, schema=schema)

        print(f"Raw JSON files discovered: {len(raw_files)}")
        print(f"Successful vulnerability records: {df.count()}")
        print(f"Skipped records: {len(failed_records)}")

        df.printSchema()

        df.write.mode("overwrite").parquet(str(output_path))

        print(f"Silver vulnerability data written to: {output_path}")
        print("OSV-to-Silver transformation completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
