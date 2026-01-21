import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

# AWS clients
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    # Required keys
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")

    if not all([session_id, project_id, campaign_id]):
        return build_response(400, {"error": "Missing required fields"})

    # Optional fields to update
    optional_fields = {
        "campaign_name": body.get("campaign_name"),
        "campaign_duration_days": body.get("campaign_duration_days"),
        "creative_brief": body.get("creative_brief"),
        "key_message": body.get("key_message"),
        "total_posts": body.get("total_posts"),
        "posts_per_week": body.get("posts_per_week")
    }

    # Get user by session_id
    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user = user_resp["Items"][0]
    user_id = user["id"]

    # Build UpdateExpression only for fields user provided
    expr_parts = []
    expr_attr_values = {}

    for i, (k, v) in enumerate(optional_fields.items()):
        if v is not None:
            placeholder = f":val{i}"
            expr_parts.append(f"{k} = {placeholder}")
            expr_attr_values[placeholder] = v

    # Always update timestamp
    expr_parts.append("updated_at = :updated_at")
    expr_attr_values[":updated_at"] = datetime.utcnow().isoformat()

    # Only run update if there is at least one field
    if expr_parts:
        update_expr = "SET " + ", ".join(expr_parts)
        campaigns_table.update_item(
            Key={
                "campaign_id": campaign_id,
                "project_id": project_id
            },
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_values
        )

    # Build response showing what was updated
    updated_fields = {k: v for k, v in optional_fields.items() if v is not None}

    response_data = {
        "campaign_id": campaign_id,
        "project_id": project_id,
        "user_id": user_id,
        **updated_fields
    }

    return build_response(
        200,
        {
            "message": "Campaign updated successfully",
            "updated_fields": response_data
        }
    )


def build_response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body)
    }
