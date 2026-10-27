
import json
import os
from pathlib import Path

import numpy as np
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas


PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "pypi"

DATABASE = "OSI_DATABASE"
SCHEMA = "GOLD"
WAREHOUSE = "OSI_WH"

load_dotenv(PROJECT_ROOT / ".env")


# All eight Gold datasets and their expected row counts.
DATASETS = {
    "package_health": {
        "table": "PACKAGE_HEALTH",
        "expected_rows": 100,
    },
    "package_dependencies": {
        "table": "PACKAGE_DEPENDENCIES",
        "expected_rows": 2344,
    },
    "package_vulnerabilities": {
        "table": "PACKAGE_VULNERABILITIES",
        "expected_rows": 33,
    },
    "package_vulnerability_assessment": {
        "table": "PACKAGE_VULNERABILITY_ASSESSMENT",
        "expected_rows": 33,
    },
    "package_risk": {
        "table": "PACKAGE_RISK",
        "expected_rows": 100,
    },
    "dependency_graph": {
        "table": "DEPENDENCY_GRAPH",
        "expected_rows": 92,
    },
    "dependency_reach": {
        "table": "DEPENDENCY_REACH",
        "expected_rows": 69,
    },
    "vulnerability_impact": {
        "table": "VULNERABILITY_IMPACT",
        "expected_rows": 100,
    },
}


def to_json_text(value):
    """Convert nested values into JSON text for Snowflake loading."""
    if value is None:
        return None

    if isinstance(value, (float, np.floating)) and pd.isna(value):
        return None

    if isinstance(value, np.ndarray):
        value = value.tolist()

    if isinstance(value, (list, dict, tuple)):
        return json.dumps(value, ensure_ascii=False, default=str)

    return value


def prepare_dataset(name, config):
    """Read and validate one local Gold dataset."""
    path = GOLD_PATH / name

    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    df = pd.read_parquet(path)

    expected = config["expected_rows"]

    if df.empty:
        raise ValueError(f"{name} is empty; refusing to load.")

    if len(df) != expected:
        raise ValueError(
            f"{name}: expected {expected} rows, found {len(df)}. "
            "No Snowflake changes have been made."
        )

    if any(not isinstance(col, str) or not col for col in df.columns):
        raise ValueError(f"{name} contains an invalid column name.")

    # Validate package_health specifically.
    if name == "package_health":
        if "package_name" not in df.columns:
            raise ValueError("package_health has no package_name column.")

        if df["package_name"].isna().any():
            raise ValueError("package_health contains missing package names.")

        if df["package_name"].duplicated().any():
            raise ValueError("package_health contains duplicate package names.")

    # Serialize nested fields where present.
    for column in ("project_urls", "direct_dependent_packages"):
        if column in df.columns:
            df[column] = df[column].apply(to_json_text)

    return df


def main():
    # Preflight all datasets before opening a Snowflake connection.
    prepared = {}

    print("Checking local Gold datasets...")

    for name, config in DATASETS.items():
        df = prepare_dataset(name, config)
        prepared[name] = df

        print(
            f"  {name}: {len(df)} rows, "
            f"{len(df.columns)} columns"
        )

    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")

    if not account or not user:
        raise ValueError(
            "SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER "
            "must be configured in the environment or .env file."
        )

    print("\nAll local checks passed. Connecting to Snowflake...")

    connection = snowflake.connector.connect(
        account=account,
        user=user,
        authenticator="OAUTH_AUTHORIZATION_CODE",
        warehouse=WAREHOUSE,
        database=DATABASE,
        schema=SCHEMA,
        login_timeout=60,
    )

    results = []

    try:
        for name, config in DATASETS.items():
            df = prepared[name]
            table = config["table"]

            print(f"\nLoading {name} -> {table}...")

            success, chunks, rows_loaded, output = write_pandas(
                conn=connection,
                df=df,
                table_name=table,
                database=DATABASE,
                schema=SCHEMA,
                auto_create_table=True,
                overwrite=True,
                quote_identifiers=False,
            )

            if not success or rows_loaded != len(df):
                raise RuntimeError(
                    f"{table}: loaded {rows_loaded} of {len(df)} rows. "
                    f"Details: {output}"
                )

            # Verify the persisted table count.
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT COUNT(*) "
                    f"FROM {DATABASE}.{SCHEMA}.{table}"
                )
                snowflake_count = cursor.fetchone()[0]

            if snowflake_count != len(df):
                raise RuntimeError(
                    f"{table}: expected {len(df)} rows in Snowflake, "
                    f"found {snowflake_count}."
                )

            results.append(
                (table, len(df), snowflake_count, chunks)
            )

            print(
                f"  Verified: {snowflake_count} rows; "
                f"{chunks} upload chunk(s)."
            )

        print("\nAll eight Gold tables loaded and verified.")
        print(f"{'TABLE':40} {'LOCAL':>8} {'SNOWFLAKE':>12}")

        for table, local_count, snowflake_count, _ in results:
            print(
                f"{table:40} {local_count:>8} "
                f"{snowflake_count:>12}"
            )

        print(f"\nTotal rows verified: {sum(r[1] for r in results)}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
