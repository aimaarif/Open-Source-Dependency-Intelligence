
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_OSV_PATH = PROJECT_ROOT / "data" / "raw" / "osv"
DETAILS_PATH = RAW_OSV_PATH / "vulnerabilities"

MAX_ATTEMPTS = 3


def find_latest_batch():
    files = list(RAW_OSV_PATH.glob(
        "ingested_date=*/run_id=*/querybatch_*.json"
    ))

    if not files:
        raise FileNotFoundError(
            "No OSV querybatch JSON file was found."
        )

    return max(files, key=lambda path: path.stat().st_mtime)


def load_vulnerability_ids(batch_path):
    envelope = json.loads(
        batch_path.read_text(encoding="utf-8")
    )

    if envelope.get("http_status") != 200:
        raise RuntimeError("The saved OSV batch was not successful.")

    queries = envelope.get("queries", [])
    response = envelope.get("response") or {}
    results = response.get("results", [])

    if len(queries) != len(results):
        raise ValueError(
            "Query and result counts differ. "
            "Cannot safely match results to packages."
        )

    # Keep the package/version association for later analysis.
    matches = {}

    for query, result in zip(queries, results):
        package = query.get("package", {})
        package_name = package.get("name")
        version = query.get("version")

        for vulnerability in result.get("vulns", []):
            vuln_id = vulnerability.get("id")

            if not vuln_id:
                continue

            matches.setdefault(vuln_id, []).append({
                "package_name": package_name,
                "version": version,
                "ecosystem": package.get("ecosystem"),
            })

    return matches


def load_cached_ids():
    """Return IDs that already have successful full records."""
    cached_ids = set()

    if not DETAILS_PATH.exists():
        return cached_ids

    for path in DETAILS_PATH.glob(
        "ingested_date=*/run_id=*/vuln_*.json"
    ):
        try:
            envelope = json.loads(
                path.read_text(encoding="utf-8")
            )

            if (
                envelope.get("http_status") == 200
                and isinstance(envelope.get("response"), dict)
                and envelope["response"].get("id")
            ):
                cached_ids.add(envelope["vulnerability_id"])

        except (OSError, json.JSONDecodeError, KeyError):
            continue

    return cached_ids


def fetch_vulnerability(vuln_id):
    url = f"https://api.osv.dev/v1/vulns/{vuln_id}"
    last_error = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        request = Request(
            url,
            headers={"Accept": "application/json"},
            method="GET",
        )

        try:
            with urlopen(request, timeout=30) as response:
                data = json.loads(
                    response.read().decode("utf-8")
                )

                return response.status, data, None

        except HTTPError as exc:
            last_error = f"HTTP {exc.code}: {exc.reason}"

            # A missing record or other permanent 4xx error
            # should be recorded, not repeatedly requested.
            if exc.code != 429 and exc.code < 500:
                return exc.code, None, last_error

        except (URLError, TimeoutError, OSError) as exc:
            last_error = str(exc)

        except json.JSONDecodeError as exc:
            last_error = f"Invalid JSON response: {exc}"

        if attempt < MAX_ATTEMPTS:
            time.sleep(2 ** attempt)

    return None, None, last_error


def main():
    batch_path = find_latest_batch()
    matches = load_vulnerability_ids(batch_path)
    cached_ids = load_cached_ids()

    pending_ids = sorted(set(matches) - cached_ids)

    print("Input batch:", batch_path)
    print("Unique vulnerability IDs:", len(matches))
    print("Already cached:", len(cached_ids.intersection(matches)))
    print("New records to fetch:", len(pending_ids))

    if not pending_ids:
        print("All vulnerability records are already cached.")
        return

    now = datetime.now(timezone.utc)
    run_id = now.strftime("%Y%m%dT%H%M%SZ")
    output_dir = (
        DETAILS_PATH
        / f"ingested_date={now.date().isoformat()}"
        / f"run_id={run_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=False)

    failures = 0

    for index, vuln_id in enumerate(pending_ids, start=1):
        status, response_data, error = fetch_vulnerability(vuln_id)

        # Keep filenames safe for filesystem use.
        safe_id = re.sub(r"[^A-Za-z0-9._-]", "_", vuln_id)

        envelope = {
            "source": "OSV.dev",
            "ingested_at": now.isoformat(),
            "run_id": run_id,
            "vulnerability_id": vuln_id,
            "request_url": f"https://api.osv.dev/v1/vulns/{vuln_id}",
            "http_status": status,
            "matched_packages": matches[vuln_id],
            "response": response_data,
            "error_message": error,
        }

        output_file = output_dir / f"vuln_{safe_id}.json"
        output_file.write_text(
            json.dumps(envelope, indent=2),
            encoding="utf-8",
        )

        if status == 200 and response_data:
            print(f"[{index}/{len(pending_ids)}] Fetched {vuln_id}")
        else:
            failures += 1
            print(
                f"[{index}/{len(pending_ids)}] FAILED {vuln_id}: "
                f"{error}"
            )

        # Modest pacing between requests.
        time.sleep(0.2)

    print("\nUnique vulnerability IDs:", len(matches))
    print("New records attempted:", len(pending_ids))
    print("Failed requests:", failures)
    print("Output directory:", output_dir)

    if failures:
        raise RuntimeError(
            "Some vulnerability records failed. "
            "Review saved error envelopes and rerun to retry failures."
        )

    print("Full vulnerability extraction completed successfully.")


if __name__ == "__main__":
    main()
