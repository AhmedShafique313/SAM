import json
import os
import time
import logging
import boto3
from botocore.exceptions import ClientError

# -------------------------------------------------
# Logging
# -------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------------------------------
# Environment Variables
# -------------------------------------------------
KNOWLEDGE_BASE_ID = "EPWIATHAOK"
DATA_SOURCE_ID = "YLPVYQZHLE"
REGION = os.environ.get("AWS_REGION", "us-east-1")

COOLDOWN_SECONDS = int(os.environ.get("INGESTION_COOLDOWN_SECONDS", "300"))
ALLOWED_PREFIXES = [
    p.strip() for p in os.environ.get("ALLOWED_PREFIXES", "").split(",") if p
]
ALLOWED_EXTENSIONS = [
    e.strip().lower() for e in os.environ.get("ALLOWED_EXTENSIONS", "").split(",") if e
]

# -------------------------------------------------
# AWS Client
# -------------------------------------------------
s3 = boto3.client("s3")
bedrock_agent = boto3.client(
    "bedrock-agent",
    region_name=REGION
)

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def is_allowed_object(key: str) -> bool:
    """Check folder and file-type filters"""
    if ALLOWED_PREFIXES and not any(key.startswith(p) for p in ALLOWED_PREFIXES):
        logger.info("Skipping object (prefix not allowed): %s", key)
        return False

    if ALLOWED_EXTENSIONS and not any(key.lower().endswith(ext) for ext in ALLOWED_EXTENSIONS):
        logger.info("Skipping object (extension not allowed): %s", key)
        return False

    return True

def get_object_metadata(bucket: str, key: str) -> dict:
    """Fetch metadata (like user_id) from S3 object"""
    try:
        response = s3.head_object(Bucket=bucket, Key=key)
        metadata = response.get("Metadata", {})
        return metadata
    except ClientError as e:
        logger.error("Failed to fetch metadata for s3://%s/%s: %s", bucket, key, e)
        return {}

def get_latest_ingestion_job():
    """Fetch the most recent ingestion job"""
    response = bedrock_agent.list_ingestion_jobs(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        dataSourceId=DATA_SOURCE_ID,
        maxResults=5
    )

    jobs = response.get("ingestionJobs", [])
    if not jobs:
        return None

    # Jobs are returned newest first
    return jobs[0]

def should_skip_ingestion() -> bool:
    """Check running ingestion or cooldown window"""
    latest_job = get_latest_ingestion_job()
    if not latest_job:
        return False

    status = latest_job["status"]
    updated_at = latest_job.get("updatedAt")

    if status == "IN_PROGRESS":
        logger.info("Ingestion already in progress. Skipping.")
        return True

    if updated_at:
        last_time = int(updated_at.timestamp())
        now = int(time.time())

        if now - last_time < COOLDOWN_SECONDS:
            logger.info(
                "Cooldown active (%ss). Skipping ingestion.",
                COOLDOWN_SECONDS
            )
            return True

    return False

def start_ingestion(trigger_key: str, metadata: dict):
    """Start KB ingestion with metadata"""
    description = f"Auto ingestion triggered by S3 object: {trigger_key}"
    if metadata.get("user_id"):
        description += f" | user_id={metadata['user_id']}"

    response = bedrock_agent.start_ingestion_job(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        dataSourceId=DATA_SOURCE_ID,
        description=description,
        # Optional: You can store metadata in the ingestion job itself if supported
        # metadata=metadata
    )

    return response["ingestionJob"]["ingestionJobId"]

# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    """
    EventBridge S3 ObjectCreated trigger
    """

    try:
        logger.info("Received event: %s", json.dumps(event))

        # Validate EventBridge S3 event
        detail = event.get("detail", {})
        bucket = detail.get("bucket", {}).get("name")
        key = detail.get("object", {}).get("key")

        if not bucket or not key:
            raise ValueError("Invalid EventBridge S3 event structure")

        logger.info("Detected upload: s3://%s/%s", bucket, key)

        # Apply filters
        if not is_allowed_object(key):
            return {
                "statusCode": 200,
                "status": "skipped",
                "reason": "Filtered object"
            }

        # Fetch metadata (user_id, project_id, etc.)
        metadata = get_object_metadata(bucket, key)
        user_id = metadata.get("user_id", "unknown")
        logger.info("Object metadata: %s", metadata)

        # Check ingestion state
        if should_skip_ingestion():
            return {
                "statusCode": 200,
                "status": "skipped",
                "reason": "Ingestion running or cooldown active",
                "user_id": user_id
            }

        # Start ingestion
        ingestion_job_id = start_ingestion(key, metadata)

        logger.info("Ingestion started. JobId=%s", ingestion_job_id)

        return {
            "statusCode": 200,
            "status": "started",
            "ingestionJobId": ingestion_job_id,
            "bucket": bucket,
            "key": key,
            "user_id": user_id
        }

    except ClientError as aws_err:
        logger.error("AWS error during ingestion", exc_info=True)
        return {
            "statusCode": 500,
            "status": "error",
            "message": str(aws_err)
        }

    except Exception as e:
        logger.error("Unhandled error", exc_info=True)
        return {
            "statusCode": 500,
            "status": "error",
            "message": str(e)
        }
