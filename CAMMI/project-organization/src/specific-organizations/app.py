import json
import boto3
from boto3.dynamodb.conditions import Attr

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")

# Tables
USERS_TABLE = "users-table"
ORGANIZATIONS_TABLE = "organizations-table"

users_table = dynamodb.Table(USERS_TABLE)
organizations_table = dynamodb.Table(ORGANIZATIONS_TABLE)


def lambda_handler(event, context):
    try:
        # Get session_id from headers
        headers = event.get("headers", {})
        session_id = headers.get("session_id")

        if not session_id:
            return response(400, {"error": "Missing session_id in headers"})

        # 1. Find the user with this session_id
        user_resp = users_table.scan(
            FilterExpression=Attr("session_id").eq(session_id)
        )

        if not user_resp["Items"]:
            return response(404, {"error": "User not found with given session_id"})

        user = user_resp["Items"][0]
        user_id = user["id"]

        # 2. Fetch organizations of the user
        org_resp = organizations_table.scan(
            FilterExpression=Attr("user_id").eq(user_id)
        )

        organizations = org_resp.get("Items", [])

        return response(200, {
            "message": "Organizations fetched successfully",
            "organizations": organizations
        })

    except Exception as e:
        return response(500, {"error": str(e)})


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, session_id"
        },
        "body": json.dumps(body)
    }
