
import hashlib
import json
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[2]

GOLD_PATH = (
    PROJECT_ROOT / "data" / "gold" / "pypi" / "package_health"
)

RAW_PATH = PROJECT_ROOT / "data" / "raw" / "osv"

API_URL = "https://api.osv.dev/v1/querybatch"

BATCH_SIZE = 100
MAX_ATTEMPTS = 3


def send_batch(queries):
    """Send one batch and retry temporary failures."""

    payload = json.dumps({"queries": queries}).encode("utf-8")

    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = Request(
            API_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        try:
            with urlopen(request, timeout=45) as response:
                status = response.status
                body = response.read().decode("utf-8")

            return status, json.loads(body), None

        except HTTPError as exc:
            error_message = f"HTTP {exc.code}: {exc.reason}"

            # Retry rate limits and temporary server errors.
            if exc.code != 429 and exc.code < 500:
                return exc.code, None, error_message

        except (URLError, TimeoutError, OSError) as exc:
            error_message = str(exc)

        except json.JSONDecodeError as exc:
            error_message = f"Invalid JSON response: {exc}"

        if attempt < MAX_ATTEMPTS:
            time.sleep(2 ** attempt)

    return None, None, error_message


def main():
    spark = (
        SparkSession.builder
        .appName("OSVVulnerabilityExtraction")
        .getOrCreate()
    )

    try:
        packages_df = spark.read.parquet(str(GOLD_PATH))

        # Keep distinct package/version combinations.
        rows = (
            packages_df
            .select("package_name", "version")
            .dropna(subset=["package_name", "version"])
            .distinct()
            .orderBy("package_name", "version")
            .collect()
        )

        queries = [
            {
                "package": {
                    "name": row["package_name"],
                    "ecosystem": "PyPI",
                },
                "version": row["version"],
            }
            for row in rows
            if str(row["package_name"]).strip()
            and str(row["version"]).strip()
        ]

        if not queries:
            raise RuntimeError("No valid package/version queries found.")

        # 3. Create a stable signature so identical queries are not sent to OSV again unnecessarily.
        canonical_queries = json.dumps(
            queries,
            sort_keys=True,
            separators=(",", ":"),
        )

        query_signature = hashlib.sha256(
            canonical_queries.encode("utf-8")
        ).hexdigest()

        # Reuse an earlier successful result for identical input.
        if RAW_PATH.exists():
            for cached_file in RAW_PATH.glob(
                "ingested_date=*/run_id=*/querybatch_*.json"
            ):
                try:
                    cached = json.loads(
                        cached_file.read_text(encoding="utf-8")
                    )

                    if (
                        cached.get("query_signature") == query_signature
                        and cached.get("http_status") == 200
                        and cached.get("response") is not None
                    ):
                        print(
                            "Matching successful OSV extraction already "
                            "exists. Skipping API request."
                        )
                        print("Cached file:", cached_file)
                        return

                except (OSError, json.JSONDecodeError):
                    continue

        # Create a unique raw-data partition.
        now = datetime.now(timezone.utc)
        ingested_at = now.isoformat()
        run_id = str(uuid.uuid4())

        output_dir = (
            RAW_PATH
            / f"ingested_date={now.date().isoformat()}"
            / f"run_id={run_id}"
        )
        output_dir.mkdir(parents=True, exist_ok=False)

        print("Distinct package/version queries:", len(queries))
        print("Run ID:", run_id)

        # 6. Query OSV in batches.
        successful_batches = 0

        for batch_number, start in enumerate(
            range(0, len(queries), BATCH_SIZE),
            start=1,
        ):
            batch = queries[start:start + BATCH_SIZE]

            status, response_data, error_message = send_batch(batch)

            # 7. Preserve the request and response, including failures.
            envelope = {
                "source": "OSV.dev",
                "ingested_at": ingested_at,
                "run_id": run_id,
                "request_url": API_URL,
                "query_signature": query_signature,
                "batch_number": batch_number,
                "query_count": len(batch),
                "http_status": status,
                "queries": batch,
                "response": response_data,
                "error_message": error_message,
            }

            output_file = (
                output_dir / f"querybatch_{batch_number:03d}.json"
            )

            output_file.write_text(
                json.dumps(envelope, indent=2),
                encoding="utf-8",
            )

            if status == 200 and response_data is not None:
                successful_batches += 1
                print(
                    f"Batch {batch_number}: success "
                    f"({len(batch)} queries)"
                )
            else:
                print(
                    f"Batch {batch_number}: FAILED "
                    f"(status={status}, error={error_message})"
                )

        print("\nSuccessful batches:", successful_batches)
        print("Total batches:", (len(queries) + BATCH_SIZE - 1) // BATCH_SIZE)
        print("Raw output:", output_dir)

        if successful_batches == (
            len(queries) + BATCH_SIZE - 1
        ) // BATCH_SIZE:
            print("Extraction completed successfully.")
        else:
            raise RuntimeError(
                "Some OSV batches failed. Inspect the saved raw files "
                "before retrying."
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
