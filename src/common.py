"""Shared config, AWS clients and load profile for every component."""

import os

import boto3
from botocore.config import Config

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://127.0.0.1:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", f"{ENDPOINT}/000000000000/ingestion-queue")
PROCESSED_QUEUE_URL = os.getenv("PROCESSED_QUEUE_URL", f"{ENDPOINT}/000000000000/processed-events-queue")
RAW_BUCKET = os.getenv("RAW_BUCKET", "scaling-raw")
REFINED_BUCKET = os.getenv("REFINED_BUCKET", "scaling-refined")
# LocalStack accepts any credentials; real AWS uses the default chain when these are unset.
ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "mock_access_key")
SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "mock_secret_key")

# Simulated day: msg/s per hour. Peak slightly above fleet capacity so the backlog
# grows during 18h-20h and drains right after.
HOURLY_RATES = [
    80, 80, 80, 80, 90, 100,
    120, 160, 200, 240, 220, 250,
    280, 240, 200, 220, 260, 300,
    450, 500, 350,  # peak
    180, 120, 80,
]


def client(service):
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=REGION,
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        config=Config(s3={"addressing_style": "path"}),
    )
