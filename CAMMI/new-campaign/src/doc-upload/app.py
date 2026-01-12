import json
import boto3
from datetime import datetime

s3 = boto3.client("s3")
BUCKET_NAME = "cammi-devprod"

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    campaign_name = body.get("campaign_name")
    file_name = body.get("file_name")

    if not campaign_name:
        return _response(400, {"error": "Missing campaign_name in request body."})
    
    if not file_name:
        file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.pdf"

    object_key = f"pdf_files/{campaign_name}/{file_name}"

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

    response_body = {
        "upload_url": presigned_url,
        "s3_path": f"s3://{BUCKET_NAME}/{object_key}",
        "file_name": file_name,
        "campaign_name": campaign_name
    }

    return _response(200, response_body)

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