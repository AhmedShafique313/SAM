import json
import boto3
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }

def generate_campaign_id():
    return uuid.uuid4().hex[:8]


def generate_campaign_name():
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    return f"Untitled Campaign - {timestamp}"


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")
    project_id = body.get("project_id")

    if not session_id or not project_id:
        return build_response(400, {
            "error": "session_id and project_id are required"
        })

    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user_id = user_resp["Items"][0]["id"]
    campaign_id = generate_campaign_id()
    campaign_name = generate_campaign_name()
    created_at = datetime.utcnow().isoformat()

    campaigns_table.put_item(
        Item={
            "campaign_id": campaign_id,        
            "project_id": project_id,           
            "user_id": user_id,
            "campaign_name": campaign_name,
            # "created_at": created_at
        }
    )

    return build_response(201, {
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "project_id": project_id
    })