
import logging
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

# Allowing Airflow to import this project's existing Python modules.
PROJECT_ROOT = "/opt/airflow/project"

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.extract.pypi import RAW_DIR, run_ingestion
from src.storage.s3 import upload_raw_files

logger = logging.getLogger(__name__)


@dag(
    dag_id="open_source_dependency_ingestion",
    description="Extract PyPI metadata, validate raw records, and upload to S3 Bronze.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["dependency-intelligence", "pypi", "bronze"],
)
def dependency_ingestion():

    @task
    def extract_pypi_metadata():
        """Run the existing PyPI extractor."""
        summary = run_ingestion()
        logger.info("Extraction summary: %s", summary)
        return summary

    @task
    def validate_extraction(summary: dict):
        """Verify that current run produce the expected JSON records."""

        if summary["total"] == 0:
            raise ValueError("The seed CSV contains no PyPI packages.")

        if summary["successful"] == 0:
            raise ValueError("All PyPI package requests failed.")

        run_id = summary["run_id"]
        run_dirs = list(
            RAW_DIR.glob(f"ingested_date=*/run_id={run_id}")
        )

        if len(run_dirs) != 1:
            raise ValueError(
                f"Expected one output directory for run {run_id}; "
                f"found {len(run_dirs)}."
            )

        json_files = list(run_dirs[0].glob("*.json"))

        if len(json_files) != summary["total"]:
            raise ValueError(
                f"Expected {summary['total']} JSON records, "
                f"but found {len(json_files)}."
            )

        if summary["failed"] > 0:
            logger.warning(
                "%s of %s package requests failed. "
                "Their error records will also be preserved.",
                summary["failed"],
                summary["total"],
            )

        logger.info(
            "Validation passed: run_id=%s, records=%s",
            run_id,
            len(json_files),
        )
        return summary

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def upload_to_s3(summary: dict):
        """Upload local raw records to S3 Bronze."""

        result = upload_raw_files()
        logger.info(
            "S3 upload result for run_id=%s: %s",
            summary["run_id"],
            result,
        )
        return result

    extraction = extract_pypi_metadata()
    validation = validate_extraction(extraction)
    upload_to_s3(validation)


dependency_ingestion()
