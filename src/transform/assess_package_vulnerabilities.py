
import json
from pathlib import Path

from packaging.version import InvalidVersion, Version
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)


def parse_json(value):
    if not value:
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return None


def assess_version(observed_version, ranges_json, versions_json):
    """Assess one candidate against explicit versions and OSV ranges."""

    def result(status, reason):
        return {"status": status, "reason": reason}

    if not observed_version:
        return result("unknown", "Observed version is missing")

    try:
        observed = Version(observed_version)
    except InvalidVersion:
        return result("unknown", "Observed version is not valid PEP 440")

    ranges = parse_json(ranges_json)
    versions = parse_json(versions_json)

    if ranges is None and ranges_json:
        return result("unknown", "Affected ranges JSON is invalid")

    if versions is None and versions_json:
        return result("unknown", "Affected versions JSON is invalid")

    if ranges is not None and not isinstance(ranges, list):
        return result("unknown", "Affected ranges is not a list")

    if versions is not None and not isinstance(versions, list):
        return result("unknown", "Affected versions is not a list")

    evaluated = False
    uncertain = False

    # Explicit version matches are direct evidence of an affected version.
    if versions:
        for item in versions:
            try:
                if Version(item) == observed:
                    return result(
                        "affected",
                        "Observed version matches an explicit OSV affected version",
                    )
            except (InvalidVersion, TypeError):
                uncertain = True

        # An explicit list can be evaluated, but ranges may extend beyond it.
        evaluated = True

    # Evaluate supported OSV ecosystem ranges.
    if ranges:
        for affected_range in ranges:
            if not isinstance(affected_range, dict):
                uncertain = True
                continue

            if affected_range.get("type") != "ECOSYSTEM":
                uncertain = True
                continue

            events = affected_range.get("events")

            if not isinstance(events, list) or not events:
                uncertain = True
                continue

            lower = None
            active = False
            intervals = []
            range_valid = True

            for event in events:
                if not isinstance(event, dict):
                    range_valid = False
                    break

                if "introduced" in event:
                    value = event["introduced"]
                    try:
                        lower = None if value == "0" else Version(value)
                        active = True
                    except (InvalidVersion, TypeError):
                        range_valid = False
                        break

                elif any(
                    key in event
                    for key in ("fixed", "last_affected", "limit")
                ):
                    key = next(
                        key
                        for key in ("fixed", "last_affected", "limit")
                        if key in event
                    )

                    if not active:
                        range_valid = False
                        break

                    try:
                        upper = Version(event[key])
                    except (InvalidVersion, TypeError):
                        range_valid = False
                        break

                    inclusive_upper = key == "last_affected"
                    intervals.append((lower, upper, inclusive_upper))
                    active = False
                    lower = None

                else:
                    range_valid = False
                    break

            if not range_valid:
                uncertain = True
                continue

            # An open-ended introduced range remains active.
            if active:
                intervals.append((lower, None, False))

            if not intervals:
                uncertain = True
                continue

            evaluated = True

            for lower, upper, inclusive_upper in intervals:
                above_lower = lower is None or observed >= lower
                below_upper = (
                    upper is None
                    or observed <= upper
                    if inclusive_upper
                    else upper is None or observed < upper
                )

                if above_lower and below_upper:
                    return result(
                        "affected",
                        "Observed version falls within an OSV ecosystem range",
                    )

    if uncertain:
        return result(
            "unknown",
            "Some affected-version information could not be evaluated safely",
        )

    if evaluated:
        return result(
            "not_affected",
            "Observed version is outside the available supported affected-version rules",
        )

    return result(
        "unknown",
        "No usable explicit versions or supported ranges were provided",
    )


def main():
    project_root = Path(__file__).resolve().parents[2]

    input_path = (
        project_root
        / "data"
        / "gold"
        / "pypi"
        / "package_vulnerabilities"
    )
    output_path = (
        project_root
        / "data"
        / "gold"
        / "pypi"
        / "package_vulnerability_assessment"
    )

    if not input_path.exists():
        raise FileNotFoundError(
            f"Candidate matches not found: {input_path}"
        )

    spark = (
        SparkSession.builder
        .appName("AssessPackageVulnerabilities")
        .getOrCreate()
    )

    try:
        candidates = spark.read.parquet(str(input_path))

        assessment_schema = StructType(
            [
                StructField("status", StringType(), False),
                StructField("reason", StringType(), False),
            ]
        )

        assess_udf = F.udf(assess_version, assessment_schema)

        evaluated = candidates.withColumn(
            "_assessment",
            assess_udf(
                F.col("observed_version"),
                F.col("affected_ranges_json"),
                F.col("affected_versions_json"),
            ),
        )

        # Multiple affected entries may exist for one advisory/package.
        # A positive match takes precedence; unresolved rules stay unknown.
        grouped = evaluated.groupBy(
            "package_name",
            "observed_version",
            "latest_snapshot_date",
            "snapshot_count",
            "observed_version_count",
            "vulnerability_id",
            "summary",
            "published",
            "modified",
            "severity_json",
        ).agg(
            F.max(
                F.when(F.col("_assessment.status") == "affected", 1)
                .otherwise(0)
            ).alias("_has_affected"),
            F.max(
                F.when(F.col("_assessment.status") == "unknown", 1)
                .otherwise(0)
            ).alias("_has_unknown"),
            F.collect_set("_assessment.reason").alias("_reasons"),
        )

        result = (
            grouped
            .withColumn(
                "version_assessment",
                F.when(
                    F.col("_has_affected") == 1, F.lit("affected")
                )
                .when(
                    F.col("_has_unknown") == 1, F.lit("unknown")
                )
                .otherwise(F.lit("not_affected")),
            )
            .withColumn(
                "assessment_reason",
                F.concat_ws("; ", F.col("_reasons")),
            )
            .drop("_has_affected", "_has_unknown", "_reasons")
        )

        print("\n=== Version Assessment Summary ===")
        print(f"Input candidate rows: {candidates.count()}")

        result.groupBy("version_assessment").count().show()

        print("\n=== Sample assessments ===")
        result.select(
            "package_name",
            "observed_version",
            "vulnerability_id",
            "version_assessment",
            "assessment_reason",
        ).show(30, truncate=False)

        result.write.mode("overwrite").parquet(str(output_path))

        print(f"Assessment dataset written to: {output_path}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
