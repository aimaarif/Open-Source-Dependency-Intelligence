
import logging
import os
from pathlib import Path

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw" / "pypi"

load_dotenv(PROJECT_ROOT / ".env")

logger = logging.getLogger(__name__)


def create_s3_client():
    """Create an authenticated S3 client using the AWS SDK."""

    profile = os.getenv("AWS_PROFILE")
    region = os.getenv("AWS_REGION")

    if not region:
        raise ValueError("AWS_REGION is not configured.")

    if profile:
        session = boto3.Session(
            profile_name=profile,
            region_name=region,
        )
    else:
        # Uses the standard AWS credential provider chain.
        session = boto3.Session(region_name=region)

    return session.client("s3")


def upload_raw_files():
    """Upload Phase 2 raw observations to the S3 Bronze bucket."""

    bucket = os.getenv("S3_BRONZE_BUCKET")

    if not bucket:
        raise ValueError("S3_BRONZE_BUCKET is not configured.")

    if not RAW_DIR.exists():
        raise FileNotFoundError(
            f"Raw data directory does not exist: {RAW_DIR}"
        )

    
    files = sorted(RAW_DIR.rglob("*.json"))

    if not files:
        raise FileNotFoundError(f"No JSON files found under {RAW_DIR}")

    client = create_s3_client()

    # Find the S3 objects that already exist.
    paginator = client.get_paginator("list_objects_v2")
    existing_keys = set()

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix="pypi/"
    ):
        for obj in page.get("Contents", []):
            existing_keys.add(obj["Key"])

    uploaded = 0
    skipped = 0

    for file_path in files:
        relative_path = file_path.relative_to(RAW_DIR).as_posix()
        key = f"pypi/{relative_path}"

        # Do not upload an object that already exists.
        if key in existing_keys:
            logger.info("Skipping existing object: s3://%s/%s", bucket, key)
            skipped += 1
            continue

        try:
            client.upload_file(
                str(file_path),
                bucket,
                key,
                ExtraArgs={
                    "ContentType": "application/json",
                    "ServerSideEncryption": "AES256",
                },
            )
            logger.info("Uploaded s3://%s/%s", bucket, key)
            uploaded += 1

        except (ClientError, BotoCoreError) as exc:
            logger.error("Upload failed for %s: %s", file_path, exc)
            raise

    result = {
        "bucket": bucket,
        "files_found": len(files),
        "files_uploaded": uploaded,
        "files_skipped": skipped,
    }

    return result

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    result = upload_raw_files()
    print(result)
