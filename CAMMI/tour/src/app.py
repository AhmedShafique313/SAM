import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from datetime import datetime

# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")

dynamodb = boto3.resource("dynamodb")

# ---------- Common Headers for CORS ----------
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
}

# ---------- Lambda Handler ----------
def lambda_handler(event, context):

    # Handle preflight (OPTIONS request)
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight check passed"})
        }

    body = json.loads(event.get("body", "{}"))

    session_id = body.get("session_id")

    # Allowed status fields from frontend
    allowed_statuses = [
        "dashboard_status",
        "user_input_status",
        "final_preview_status",
        "document_preview_status"
    ]

    # Identify which status is sent
    status_key = None
    status_value = None
    for key in allowed_statuses:
        if key in body:
            status_key = key
            status_value = body[key]
            break

    if not session_id or status_key is None:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "session_id and one status field are required"
            })
        }

    users_table = dynamodb.Table(USERS_TABLE)

    # Scan table using session_id
    resp = users_table.scan(
        FilterExpression=Attr("session_id").eq(session_id)
    )
    items = resp.get("Items", [])

    if not items:
        return {
            "statusCode": 404,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "User not found for given session_id"
            })
        }

    user = items[0]

    # Use PRIMARY KEY (email) for update
    email = user.get("email")

    if not email:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "User record does not contain email (primary key)"
            })
        }

    # Update only the received status
    users_table.update_item(
        Key={"email": email},
        UpdateExpression=f"SET {status_key} = :val, updated_at = :updated_at",
        ExpressionAttributeValues={
            ":val": status_value,
            ":updated_at": datetime.utcnow().isoformat()
        }
    )

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "message": "Status updated successfully",
            "email": email,
            "updated_status": {
                status_key: status_value
            }
        })
    }
