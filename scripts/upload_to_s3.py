import os
from pathlib import Path
import boto3
from dotenv import load_dotenv

# =========================
# Load environment variables
# =========================
# This automatically finds the .env file in the same directory
load_dotenv(override=True)

# =========================
# Configuration
# =========================
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "ecommerce_data")
BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "raw/ecommerce")

# =========================
# Create S3 client
# =========================
# Boto3 automatically picks up AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from the .env file
s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)

# =========================
# Upload files
# =========================
def upload_directory_to_s3(local_directory, bucket_name, s3_prefix):
    local_directory = Path(local_directory)

    if not local_directory.exists():
        raise FileNotFoundError(
            f"Directory not found: {local_directory}"
        )

    files = [
        file
        for file in local_directory.rglob("*")
        if file.is_file()
    ]

    if not files:
        print("No files found.")
        return

    print(f"Found {len(files)} files.\n")

    for file_path in files:
        relative_path = file_path.relative_to(local_directory)
        s3_key = f"{s3_prefix}/{relative_path.as_posix()}"

        print(
            f"Uploading: {file_path} "
            f"-> s3://{bucket_name}/{s3_key}"
        )

        s3.upload_file(
            str(file_path),
            bucket_name,
            s3_key
        )

        print("Uploaded successfully.\n")


# =========================
# Main
# =========================
if __name__ == "__main__":
    if not BUCKET_NAME:
        raise ValueError("AWS_BUCKET_NAME is not set in the .env file.")
        
    upload_directory_to_s3(
        OUTPUT_DIR,
        BUCKET_NAME,
        S3_PREFIX
    )

    print("================================")
    print("All files uploaded successfully")
    print("================================")