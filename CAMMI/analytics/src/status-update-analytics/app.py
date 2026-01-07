import json
import boto3
from boto3.dynamodb.conditions import Key

# ---------- CONFIG ----------
USERS_TABLE_NAME = "users-table"
SUPPORT_TABLE_NAME = "email-support-table"
SESSION_GSI_NAME = "session_id-index"

# ---------- AWS CLIENTS ----------
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table(USERS_TABLE_NAME)
support_table = dynamodb.Table(SUPPORT_TABLE_NAME)

# ---------- CORS HEADERS ----------
cors_headers = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST"
}

# ---------- GET USER ID USING SESSION ID ----------
def get_user_id_from_session(session_id):
    response = users_table.query(
        IndexName=SESSION_GSI_NAME,
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    items = response.get("Items", [])
    if not items:
        raise Exception("Invalid or expired session_id")

    return items[0]["id"]  # user_id


# ---------- CHECK IF SUPPORT TICKET EXISTS ----------
def check_support_ticket_exists(user_id):
    response = support_table.query(
        KeyConditionExpression=Key("user_id").eq(user_id),
        Limit=1
    )

    return len(response.get("Items", [])) > 0


# ---------- LAMBDA HANDLER ----------
def lambda_handler(event, context):

    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": ""
        }

    try:
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")

        if not session_id:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": "session_id is required"})
            }

        # Step 1: Get user_id from session_id
        user_id = get_user_id_from_session(session_id)

        # Step 2: Check support table
        record_exists = check_support_ticket_exists(user_id)

        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": json.dumps({
                "record_found": "YES" if record_exists else "NO"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": cors_headers,
            "body": json.dumps({"error": str(e)})
        }
