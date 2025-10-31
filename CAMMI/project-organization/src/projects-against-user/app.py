import json
import boto3
import os
from boto3.dynamodb.conditions import Attr, Key
 
dynamodb = boto3.resource("dynamodb")
 
# Environment variables
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
ORGANIZATIONS_TABLE = os.environ.get("ORGANIZATIONS_TABLE", "organizations-table")
ALLOWED_ORIGINS = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",")]
 
users_table = dynamodb.Table(USERS_TABLE)
organizations_table = dynamodb.Table(ORGANIZATIONS_TABLE)
 
def cors_headers(origin):
    if origin in ALLOWED_ORIGINS:
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Access-Control-Allow-Headers": "Content-Type,x-session-id,session_id",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE",
        }
    return {
        "Access-Control-Allow-Origin": ALLOWED_ORIGINS[0],
        "Access-Control-Allow-Credentials": "true",
        "Vary": "Origin",
    }
 
def lambda_handler(event, context):
    origin = (event.get("headers") or {}).get("origin") or (event.get("headers") or {}).get("Origin")
    headers = cors_headers(origin)
 
    # Handle preflight request
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}
 
    try:
        # 1. Validate session_id
        session_id = (event.get("headers") or {}).get("session_id")
        if not session_id:
            return {"statusCode": 400, "headers": headers, "body": json.dumps({"error": "Missing session_id"})}
 
        # 2. Check if user exists and get user_id
        # user_response = users_table.scan(
        #     FilterExpression=Attr("session_id").eq(session_id),
        #     Limit=1
        # )
        user_response = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id)
        )
        if not user_response["Items"]:
            return {"statusCode": 401, "headers": headers, "body": json.dumps({"error": "Unauthorized: Invalid session_id"})}
 
        user_id = user_response["Items"][0]["id"]
 
        # 3. Fetch all projects for this user
        projects_response = organizations_table.scan(
            FilterExpression=Attr("user_id").eq(user_id)
        )
 
        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps({
                "user_id": user_id,
                "projects": projects_response.get("Items", [])
            })
        }
 
    except Exception as e:
        return {"statusCode": 500, "headers": headers, "body": json.dumps({"error": str(e)})}