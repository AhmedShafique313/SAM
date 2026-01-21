import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
BUCKET_NAME = "cammi-devprod"
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")
    file_name = body.get("file_name")  # Optional

    if not session_id or not project_id or not campaign_id:
        return _response(400, {"error": "Missing session_id, campaign_id and project_id in request body."})
    
    if not file_name:
        file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.pdf"

    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user_id = user_resp["Items"][0]["id"]

    object_key = f"pdf_files/{project_id}/{user_id}/{campaign_id}/{file_name}"

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
        "session_id": session_id,
        "project_id": project_id,
        "campaign_id": campaign_id
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