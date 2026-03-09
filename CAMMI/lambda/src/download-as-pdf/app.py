import json, os
import boto3
import urllib3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)

# AWS clients
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
http = urllib3.PoolManager()

# Hardcoded config
USERS_TABLE = "users-table"
PROJECT_STATE_TABLE = "project-state-table"
BUCKET_NAME = "cammi-devprod"

CONVERTAPI_KEY = "XmRpqHvNAy6NmDm0UPYgbPtVeCfXvkhe"

CONVERTAPI_URL = "https://v2.convertapi.com/convert/docx/to/pdf?Secret=" + CONVERTAPI_KEY

# CORS headers
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, session_id",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
}

# Credit cost for this operation
CREDIT_COST = 2

# DynamoDB tables
users_table = dynamodb.Table(USERS_TABLE)
project_state_table = dynamodb.Table(PROJECT_STATE_TABLE)


def lambda_handler(event, context):
    body_str = event.get("body", "{}")
    body = json.loads(body_str)

    session_id = body.get("session_id")
    project_id = body.get("project_id")

    if not session_id or not project_id:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing session_id or project_id"}),
            "headers": CORS_HEADERS,
        }

    # Validate session_id and get user using the session_id index
    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression="session_id = :session_id",
        ExpressionAttributeValues={
            ":session_id": session_id
        },
        ProjectionExpression="email, total_credits"
    )

    if "Items" not in user_resp or len(user_resp["Items"]) == 0:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "Invalid session_id"}),
            "headers": CORS_HEADERS,
        }

    user = user_resp["Items"][0]
    user_email = user.get("email")
    total_credits = user.get("total_credits", 0)
    
    # Convert total_credits to int for comparison and later use
    if isinstance(total_credits, Decimal):
        total_credits = int(total_credits)

    # Check if user has enough credits
    if total_credits < CREDIT_COST:
        return {
            "statusCode": 403,
            "body": json.dumps({
                "error": "Insufficient credits",
                "required_credits": CREDIT_COST,
                "available_credits": total_credits
            }),
            "headers": CORS_HEADERS,
        }

    # Fetch generating_document from project-state-table
    project_resp = project_state_table.get_item(
        Key={"project_id": project_id}
    )

    if "Item" not in project_resp or "generating_document" not in project_resp["Item"]:
        return {
            "statusCode": 404,
            "body": json.dumps({"error": "generating_document not found for project"}),
            "headers": CORS_HEADERS,
        }

    document_type = project_resp["Item"]["generating_document"]

    if not document_type:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "generating_document is empty"}),
            "headers": CORS_HEADERS,
        }

    # Convert to lowercase
    document_type = document_type.lower()

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

    # Pick latest file
    latest_file = max(docx_files, key=lambda x: x["LastModified"])
    docx_key = latest_file["Key"]

    # Read file from S3
    s3_object = s3.get_object(Bucket=BUCKET_NAME, Key=docx_key)
    file_bytes = s3_object["Body"].read()

    # Multipart form build
    boundary = "----LambdaBoundary"
    body_multipart = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="File"; filename="{docx_key}"\r\n'
        f"Content-Type: application/vnd.openxmlformats-officedocument.wordprocessingml.document\r\n\r\n"
    ).encode("utf-8") + file_bytes + f"\r\n--{boundary}--\r\n".encode("utf-8")

    headers_req = {"Content-Type": f"multipart/form-data; boundary={boundary}"}

    # Call ConvertAPI
    response = http.request("POST", CONVERTAPI_URL, body=body_multipart, headers=headers_req)
    result = json.loads(response.data.decode("utf-8"))

    # Check if conversion was successful before deducting credits
    if "Files" in result and "FileData" in result["Files"][0]:
        # Deduct credits only on successful conversion
        try:
            new_credit_balance = total_credits - CREDIT_COST
            users_table.update_item(
                Key={"email": user_email},
                UpdateExpression="SET total_credits = :credits",
                ConditionExpression="total_credits = :current_credits",
                ExpressionAttributeValues={
                    ":credits": Decimal(str(new_credit_balance)),
                    ":current_credits": Decimal(str(total_credits))
                }
            )
        except Exception as e:
            # Handle potential race condition where credits changed
            return {
                "statusCode": 409,
                "body": json.dumps({
                    "error": "Credit balance changed. Please try again.",
                    "details": str(e)
                }),
                "headers": CORS_HEADERS,
            }

        base64_pdf = result["Files"][0]["FileData"]
        pdf_file_name = docx_key.replace(".docx", ".pdf").split("/")[-1]

        return {
            "statusCode": 200,
            "body": json.dumps({
                "fileName": pdf_file_name,
                "base64_pdf": base64_pdf,
                "credits_deducted": CREDIT_COST,
                "remaining_credits": new_credit_balance
            }, cls=DecimalEncoder),  # Use custom encoder for Decimal objects
            "headers": CORS_HEADERS,
        }

    return {
        "statusCode": 400,
        "body": json.dumps({"error": "Conversion failed", "details": result}),
        "headers": CORS_HEADERS,
    }