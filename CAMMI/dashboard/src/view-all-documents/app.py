import json
import boto3
from boto3.dynamodb.conditions import Attr, Key

dynamodb = boto3.resource('dynamodb')

USERS_TABLE = "users-table"
DOCUMENT_HISTORY_TABLE = "documents-history-table"

def lambda_handler(event, context):
    # ✅ Handle OPTIONS request for CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return _cors_response(200, {})
    
    try:
        # ✅ Parse input
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        if not session_id:
            return _error_response("Missing session_id in request body")

        # ✅ Step 1: Find user by session_id (using scan since it's not a key)
        users_table = dynamodb.Table(USERS_TABLE)
        user_response = users_table.scan(
            FilterExpression=Attr("session_id").eq(session_id)
        )

        user_items = user_response.get("Items", [])
        if not user_items:
            return _error_response(f"No user found for session_id: {session_id}")

        user_item = user_items[0]
        user_id = user_item.get("id")

        if not user_id:
            return _error_response("User record missing 'id' field")

        # ✅ Step 2: Get all document history records for that user_id
        doc_table = dynamodb.Table(DOCUMENT_HISTORY_TABLE)
        doc_response = doc_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id)
        )
        documents = doc_response.get("Items", [])

        # ✅ Step 3: Return final response with CORS headers
        return _cors_response(200, {
            "session_id": session_id,
            "user_id": user_id,
            "documents": documents  # Changed from document_history to match your frontend
        })

    except Exception as e:
        return _error_response(str(e))


def _cors_response(status_code, body_dict):
    """Return response with proper CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",  # Change to specific domain in production
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Content-Type": "application/json"
        },
        "body": json.dumps(body_dict)
    }


def _error_response(message, code=400):
    """Return error response with CORS headers"""
    return _cors_response(code, {"error": message})