import json, os
import boto3
import urllib3
from boto3.dynamodb.conditions import Attr

# AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
http = urllib3.PoolManager()

# Hardcoded config (⚠️ not recommended for production)
USERS_TABLE = "users-table"
BUCKET_NAME = "cammi-devprod"
CONVERTAPI_KEY = os.environ["CONVERTAPI_KEY"]

# API URL
CONVERTAPI_URL = "https://v2.convertapi.com/convert/docx/to/pdf?Secret=" + CONVERTAPI_KEY

# CORS headers
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, session_id",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
}

# DynamoDB table reference
users_table = dynamodb.Table(USERS_TABLE)


def lambda_handler(event, context):
    # Extract headers
    headers = event.get("headers", {})
    session_id = headers.get("session_id")
    project_id = headers.get("project_id")
    document_type = headers.get("document_type")

    if not session_id or not project_id or not document_type:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing session_id, project_id, or document_type"}),
            "headers": CORS_HEADERS,
        }

    # Validate session_id in DynamoDB
    user_resp = users_table.scan(
        FilterExpression=Attr("session_id").eq(session_id),
        ProjectionExpression="id"
    )
    if "Items" not in user_resp or len(user_resp["Items"]) == 0:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "Invalid session_id"}),
            "headers": CORS_HEADERS,
        }

    # Build S3 folder prefix
    folder_prefix = f"project/{project_id}/{document_type}/marketing_strategy_document/"

    # Find .docx files in S3
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_prefix)
    docx_files = [obj for obj in response.get("Contents", []) if obj["Key"].endswith(".docx")]

    if not docx_files:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "No .docx files found"}),
            "headers": CORS_HEADERS,
        }

    # Pick the latest file
    latest_file = max(docx_files, key=lambda x: x["LastModified"])
    docx_key = latest_file["Key"]

    # Read file from S3
    s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=docx_key)
    file_bytes = s3_object["Body"].read()

    # Build multipart/form-data for ConvertAPI
    boundary = "----LambdaBoundary"
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="File"; filename="{docx_key}"\r\n'
        f"Content-Type: application/vnd.openxmlformats-officedocument.wordprocessingml.document\r\n\r\n"
    ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

    headers_req = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

    # Call ConvertAPI
    response = http.request("POST", CONVERTAPI_URL, body=body, headers=headers_req)
    result = json.loads(response.data.decode("utf-8"))

    # Return PDF as base64
    if "Files" in result and "FileData" in result["Files"][0]:
        base64_pdf = result["Files"][0]["FileData"]
        pdf_file_name = docx_key.replace(".docx", ".pdf").split("/")[-1]

        return {
            "statusCode": 200,
            "body": json.dumps({
                "fileName": pdf_file_name,
                "base64_pdf": base64_pdf
            }),
            "headers": CORS_HEADERS,
        }

    return {
        "statusCode": 400,
        "body": json.dumps({"error": "Conversion failed", "details": result}),
        "headers": CORS_HEADERS,
    }

