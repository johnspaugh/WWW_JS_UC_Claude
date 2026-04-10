import os
import boto3
from dotenv import load_dotenv

load_dotenv()

UPLOAD_BUCKET   = os.getenv("S3_UPLOAD_BUCKET")
INTERNAL_BUCKET = os.getenv("S3_STORAGE_BUCKET")
ENDPOINT        = os.getenv("S3_ENDPOINT")


def get_client():
    """Return a boto3 S3 client using the configured profile and endpoint."""
    session = boto3.Session(profile_name=os.getenv("AWS_PROFILE"))
    return session.client("s3", endpoint_url=ENDPOINT)
