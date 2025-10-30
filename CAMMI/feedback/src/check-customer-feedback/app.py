import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Table names
USERS_TABLE = "users-table"
FEEDBACK_TABLE = "users-table-feedback"

def lambda_handler(event, context):
    # Parse input
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")

    if not session_id:
        return _response(400, "Missing session_id in request body.")

    # Step 1: Find user record using session_id (not the partition key)
    users_table = dynamodb.Table(USERS_TABLE)

    # Since 'session_id' is not the partition key, we use a Scan with a filter expression
    user_resp = users_table.scan(
        FilterExpression=Attr('session_id').eq(session_id)
    )
    users = user_resp.get("Items", [])

    if not users:
        return _response(404, "User not found for given session_id.")

    # Assume one user per session_id
    user_item = users[0]
    user_id = user_item.get("id")

    if not user_id:
        return _response(400, "User record missing 'id' field.")

    # Step 2: Query feedback table for this user_id
    feedback_table = dynamodb.Table(FEEDBACK_TABLE)
    feedback_resp = feedback_table.query(
        KeyConditionExpression=Key('user_id').eq(user_id)
    )
    feedback_items = feedback_resp.get("Items", [])

    # Step 3: Return result
    if feedback_items:
        return _response(200, "Done")
    else:
        return _response(200, "Pending")

# Helper to format Lambda responses
def _response(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps({"message": message})
    }
