import json
import boto3
import logging
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')

# Set your actual JSON file location in S3
LOOKUP_BUCKET = "cammi-devprod"


def extract_key_from_s3_path(s3_path):
    parsed = urlparse(s3_path)
    parts = parsed.path.strip("/").split("/")
    try:
        output_index = parts.index("output")
        return "/".join(parts[output_index + 1:-1])  # skip "output", skip filename
    except ValueError:
        return None

def lambda_handler(event, context):
    subheading = event.get("subheading", "").strip().lower()
    prompt = event.get("prompt", "").strip()
    session_id = event.get("session_id", "")
    user_id = event.get("user_id", "")
    document_type = event.get("document_type", "")
    project_id = event.get("project_id", "")
    LOOKUP_KEY = f"flow/{document_type}/marketing_document_template.json"  # e.g. "lookup/section_map.json"


    if not subheading:
        return {
            "statusCode": 400,
            "body": "Missing required field: 'subheading'"
        }

    # Load the JSON file from S3
    try:
        print("This is bucket URL",LOOKUP_KEY)
        obj = s3.get_object(Bucket=LOOKUP_BUCKET, Key=LOOKUP_KEY)
        data = json.loads(obj['Body'].read())
    except Exception as e:
        logger.error(f"Error reading JSON from S3: {e}")
        return {
            "statusCode": 500,
            "body": "Failed to read mapping file from S3"
        }

    # Search for subheading match in any section
    for section in data:
        for item in section.get("sections", []):
            if item.get("subheading", "").strip().lower() == subheading:
                s3_path = item.get("s3_path")
                key = extract_key_from_s3_path(s3_path)
                return {
                    "statusCode": 200,
                    "key": key,
                    "prompt": prompt,
                    "edit_flag": True,
                    "session_id": session_id,
                    "user_id": user_id,
                    "project_id": project_id,
                    "document_type": document_type
                }

    return {
        "statusCode": 404,
        "body": f"No match found for subheading '{subheading}'"
    }
