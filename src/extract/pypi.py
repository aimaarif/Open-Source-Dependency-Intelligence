
import csv
import json
import logging
import re
import time
import uuid

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SEED_FILE = PROJECT_ROOT / "config" / "package_seed.csv"
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "pypi"

BASE_URL = "https://pypi.org/pypi"
TIMEOUT_SECONDS = 15
MAX_ATTEMPTS = 3

RETRYABLE_STATUSES = {429, 500, 502, 503, 504}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


def fetch_package(
    package_name,
    session=None,
    max_attempts=MAX_ATTEMPTS,
    timeout=TIMEOUT_SECONDS,
):
    """Fetch one package's raw metadata from PyPI."""

    if not package_name or not package_name.strip():
        raise ValueError("Package name cannot be empty.")

    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1.")

    package_name = package_name.strip()
    encoded_name = quote(package_name, safe="")
    url = f"{BASE_URL}/{encoded_name}/json"

    client = session or requests.Session()

    for attempt in range(1, max_attempts + 1):
        try:
            response = client.get(url, timeout=timeout)

            if (
                response.status_code in RETRYABLE_STATUSES
                and attempt < max_attempts
            ):
                retry_after = response.headers.get("Retry-After")

                try:
                    delay = min(float(retry_after), 60) if retry_after else min(
                        2 ** (attempt - 1), 30
                    )
                except ValueError:
                    delay = min(2 ** (attempt - 1), 30)

                logger.warning(
                    "Retryable HTTP %s for %s; attempt %s/%s",
                    response.status_code,
                    package_name,
                    attempt,
                    max_attempts,
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response.json(), response.status_code, url

        except (requests.Timeout, requests.ConnectionError) as exc:
            if attempt == max_attempts:
                raise

            delay = min(2 ** (attempt - 1), 30)

            logger.warning(
                "Network error for %s; attempt %s/%s: %s",
                package_name,
                attempt,
                max_attempts,
                exc,
            )
            time.sleep(delay)

    # Normally unreachable; protects against unexpected loop changes.
    raise RuntimeError(f"Could not fetch {package_name}")


def load_seed_packages():
    """Read and validate package names from the seed CSV."""

    packages = []

    with SEED_FILE.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        required_columns = {"package_name", "ecosystem"}

        if not reader.fieldnames or not required_columns.issubset(
            reader.fieldnames
        ):
            raise ValueError(
                "Seed CSV must contain package_name and ecosystem columns."
            )

        for row in reader:
            package_name = (row.get("package_name") or "").strip()
            ecosystem = (row.get("ecosystem") or "").strip().lower()

            if not package_name:
                logger.warning("Skipping row with an empty package name.")
                continue

            if ecosystem != "pypi":
                continue

            packages.append(package_name)

    # Preserve input order while removing duplicate package names.
    return list(dict.fromkeys(packages))


def save_observation(
    package_name,
    run_id,
    ingested_at,
    payload=None,
    http_status=None,
    request_url=None,
    error=None,
):
    """Save one package observation without overwriting previous runs."""

    date_partition = ingested_at[:10]

    # Allow ordinary Python package names, not arbitrary file paths.
    safe_name = re.sub(r"[^A-Za-z0-9._-]", "_", package_name)

    output_dir = (
        RAW_DIR
        / f"ingested_date={date_partition}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{safe_name}.json"

    observation = {
        "source": "pypi",
        "package_name": package_name,
        "ingested_at": ingested_at,
        "run_id": run_id,
        "request_url": request_url,
        "http_status": http_status,
        "payload": payload,
        "error_message": error,
    }

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(
            observation,
            file,
            ensure_ascii=False,
            indent=2,
        )

    return output_file


def run_ingestion():
    """Collect metadata for every package in the seed CSV."""

    packages = load_seed_packages()

    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    successful = 0
    failed = 0

    logger.info(
        "Starting PyPI ingestion: run_id=%s, packages=%s",
        run_id,
        len(packages),
    )

    with requests.Session() as session:
        for package_name in packages:
            ingested_at = datetime.now(timezone.utc).isoformat()
            request_url = (
                f"{BASE_URL}/{quote(package_name, safe='')}/json"
            )

            payload = None
            http_status = None
            error = None

            try:
                payload, http_status, request_url = fetch_package(
                    package_name,
                    session=session,
                )

                if not isinstance(payload, dict):
                    raise ValueError("PyPI response must be a JSON object.")

                if not isinstance(payload.get("info"), dict):
                    raise ValueError("PyPI response has no valid info object.")

                successful += 1
                logger.info("Fetched package: %s", package_name)

            except requests.HTTPError as exc:
                error = str(exc)

                if exc.response is not None:
                    http_status = exc.response.status_code

                failed += 1
                logger.error("HTTP failure for %s: %s", package_name, exc)

            except (
                requests.RequestException,
                ValueError,
            ) as exc:
                error = str(exc)
                failed += 1
                logger.error("Failed to ingest %s: %s", package_name, exc)

            except Exception:
                # Preserve an unexpected failure record, then stop the run
                # rather than silently hiding a programming error.
                save_observation(
                    package_name,
                    run_id,
                    ingested_at,
                    http_status=http_status,
                    request_url=request_url,
                    error="Unexpected error; inspect application logs.",
                )
                raise

            save_observation(
                package_name=package_name,
                run_id=run_id,
                ingested_at=ingested_at,
                payload=payload,
                http_status=http_status,
                request_url=request_url,
                error=error,
            )

    logger.info(
        "Ingestion finished: run_id=%s, started_at=%s, "
        "successful=%s, failed=%s",
        run_id,
        started_at,
        successful,
        failed,
    )

    return {
        "run_id": run_id,
        "total": len(packages),
        "successful": successful,
        "failed": failed,
    }


if __name__ == "__main__":
    summary = run_ingestion()
    print(json.dumps(summary, indent=2))
