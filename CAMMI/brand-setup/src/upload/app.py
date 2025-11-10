import json
import boto3
import os
from datetime import datetime

s3 = boto3.client("s3")

# Fixed S3 bucket name
BUCKET_NAME = "cammi-devprod"

def lambda_handler(event, context):
    # Parse input body from event
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    file_name = body.get("file_name")  # Optional

    if not session_id or not project_id:
        return _response(400, {"error": "Missing session_id and project_id in request body."})
    
    if not file_name:
        file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.pdf"

    # Construct S3 path
    object_key = f"pdf_files/{project_id}/{session_id}/{file_name}"

    # Generate pre-signed URL (valid 5 minutes)
    presigned_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": object_key,
            "ContentType": "application/pdf"
        },
        ExpiresIn=300,
        HttpMethod="PUT"
    )

    # Response payload
    response_body = {
        "upload_url": presigned_url,
        "s3_path": f"s3://{BUCKET_NAME}/{object_key}",
        "file_name": file_name,
        "session_id": session_id,
        "project_id": project_id
    }

    return _response(200, response_body)


# ✅ Helper function for standardized responses (CORS + JSON)
def _response(status_code, body_obj):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body_obj)
    }
