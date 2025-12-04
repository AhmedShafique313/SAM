import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError
from datetime import datetime

# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
ONBOARDING_TABLE = os.environ.get("ONBOARDING_TABLE", "onboarding-questions-table")

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")

# ---------- Common Headers for CORS ----------
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",   # You can replace "*" with specific domain for more security
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
}
 
 
# ---------- Ensure Onboarding table exists ----------
def ensure_onboarding_table():
    try:
        client.describe_table(TableName=ONBOARDING_TABLE)
    except client.exceptions.ResourceNotFoundException:
        dynamodb.create_table(
            TableName=ONBOARDING_TABLE,
            KeySchema=[
                {"AttributeName": "user_id", "KeyType": "HASH"},   # Partition key
                {"AttributeName": "question", "KeyType": "RANGE"}  # Sort key
            ],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "S"},
                {"AttributeName": "question", "AttributeType": "S"}
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
        )
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=ONBOARDING_TABLE)
 
 
# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        # Handle preflight (OPTIONS request)
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": json.dumps({"message": "CORS preflight check passed"})
            }
 
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        question = body.get("question")
        answer = body.get("answer")
 
        if not session_id or not question or not answer:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"message": "session_id, question and answer are required"})
            }
 
        # Ensure Onboarding table exists
        ensure_onboarding_table()
 
        # Lookup user_id from Users table using session_id
        users_table = dynamodb.Table(USERS_TABLE)
        resp = users_table.scan(
            FilterExpression=Attr("session_id").eq(session_id)
        )
        items = resp.get("Items", [])
 
        if not items:
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({"message": "User not found for given session_id"})
            }
 
        user = items[0]
        user_id = user.get("id")
 
        if not user_id:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"message": "User record does not contain user_id"})
            }
 
        # Current timestamp
        now = datetime.utcnow().isoformat()
 
        onboarding_table = dynamodb.Table(ONBOARDING_TABLE)
 
        # First try to insert with created_at if not exists
        try:
            onboarding_table.update_item(
                Key={"user_id": user_id, "question": question},
                UpdateExpression="SET answer = :a, created_at = if_not_exists(created_at, :c), updated_at = :u",
                ExpressionAttributeValues={
                    ":a": answer,
                    ":c": now,
                    ":u": now
                },
                ReturnValues="ALL_NEW"
            )
        except ClientError as e:
            print("DynamoDB error:", str(e))
            return {
                "statusCode": 500,
                "headers": CORS_HEADERS,
                "body": json.dumps({"message": "Failed to update onboarding data", "error": str(e)})
            }
 
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Onboarding data stored/updated successfully", "user_id": user_id})
        }
 
    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Server error", "error": str(e)})
        }