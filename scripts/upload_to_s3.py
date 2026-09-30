"""
upload_to_s3.py

Uploads the raw CSV files to S3 under the raw/<table>/ prefix layout
that the Snowflake external stage expects. Creates the bucket if it
does not already exist.

Usage:
    python scripts/upload_to_s3.py

Dependencies:
    pip install boto3 python-dotenv

Required env vars (set these in .env):
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, S3_BUCKET
"""

import os
import sys
import boto3
from botocore.exceptions import ClientError
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION    = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
S3_BUCKET             = os.getenv("S3_BUCKET", "food-intel-datalake")

# Root of the local data folder (one level above scripts/)
DATA_DIR = Path(__file__).parent.parent / "Data"

# Maps local filename (no extension) to the S3 prefix under raw/
# Keeping restaurant -> restaurants to match the Snowflake stage path
FILE_MAP = {
    "food"        : "food",
    "menu"        : "menu",
    "order_items" : "order_items",
    "orders"      : "orders",
    "restaurant"  : "restaurants",
    "reviews"     : "reviews",
    "users"       : "users",
}


def check_env():
    missing = [v for v in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET")
               if not os.getenv(v)]
    if missing:
        print(f"[ERROR] Missing env vars: {', '.join(missing)}")
        print("        Copy .env.example to .env and fill in the values.")
        sys.exit(1)

    if not DATA_DIR.exists():
        print(f"[ERROR] Data folder not found: {DATA_DIR}")
        sys.exit(1)


def bucket_exists(client, bucket):
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            return False
        if code == "403":
            print(f"[ERROR] Bucket '{bucket}' exists but access is denied. Check IAM.")
            sys.exit(1)
        raise


def create_bucket(client, bucket, region):
    try:
        if region == "us-east-1":
            client.create_bucket(Bucket=bucket)
        else:
            client.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"  Bucket created: s3://{bucket} ({region})")
    except ClientError as e:
        print(f"[ERROR] Could not create bucket: {e}")
        print("        Make sure your IAM user has the s3:CreateBucket permission.")
        sys.exit(1)


def file_size_mb(path):
    return path.stat().st_size / (1024 * 1024)


def upload_with_progress(client, local_path, s3_key, bucket):
    """Uploads a file and prints a simple inline progress bar."""
    total = local_path.stat().st_size
    done  = [0]

    def callback(n):
        done[0] += n
        pct    = done[0] / total * 100
        blocks = int(pct / 5)
        bar    = "#" * blocks + "-" * (20 - blocks)
        print(f"\r    [{bar}] {pct:5.1f}%  {done[0]/1024/1024:.1f} MB", end="", flush=True)

    try:
        client.upload_file(str(local_path), bucket, s3_key, Callback=callback)
        print()
        return True
    except ClientError as e:
        print(f"\n    [ERROR] {e}")
        return False


def already_exists(client, bucket, key):
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def main():
    check_env()

    print("-" * 55)
    print("  Food-Intel  |  Upload raw CSVs to S3")
    print("-" * 55)
    print(f"  bucket : s3://{S3_BUCKET}")
    print(f"  region : {AWS_DEFAULT_REGION}")
    print(f"  source : {DATA_DIR}")
    print("-" * 55)

    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION,
    )

    if bucket_exists(client, S3_BUCKET):
        print(f"\n  Bucket found: s3://{S3_BUCKET}")
    else:
        print(f"\n  Bucket not found. Creating s3://{S3_BUCKET} ...")
        create_bucket(client, S3_BUCKET, AWS_DEFAULT_REGION)

    uploaded = []
    skipped  = []
    failed   = []
    missing  = []

    print()
    for name, prefix in FILE_MAP.items():
        local = DATA_DIR / f"{name}.csv"
        key   = f"raw/{prefix}/{name}.csv"

        print(f"  {name}.csv  ->  s3://{S3_BUCKET}/{key}")

        if not local.exists():
            print(f"    not found locally, skipping\n")
            missing.append(name)
            continue

        print(f"    size: {file_size_mb(local):.1f} MB")

        if already_exists(client, S3_BUCKET, key):
            print(f"    already in S3, skipping\n")
            skipped.append(name)
            continue

        if upload_with_progress(client, local, key, S3_BUCKET):
            print(f"    done\n")
            uploaded.append(name)
        else:
            failed.append(name)
            print()

    print("-" * 55)
    print("  Results")
    print("-" * 55)
    print(f"  uploaded : {len(uploaded)}  {uploaded}")
    print(f"  skipped  : {len(skipped)}  {skipped}")
    print(f"  missing  : {len(missing)}  {missing}")
    print(f"  failed   : {len(failed)}  {failed}")
    print("-" * 55)

    print(f"\n  s3://{S3_BUCKET}/raw/")
    for name, prefix in FILE_MAP.items():
        status = "ok" if (name in uploaded or name in skipped) else "not uploaded"
        print(f"    {prefix}/{name}.csv  [{status}]")

    if failed:
        print("\n  Some files failed to upload. See errors above.")
        sys.exit(1)

    print("\n  All files in S3. Ready to run COPY INTO in Snowflake.\n")


if __name__ == "__main__":
    main()