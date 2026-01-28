import json
import boto3
import math
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key

# AWS clients
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")


def normalize_number(value):
    """
    Convert DynamoDB Decimal to int for JSON serialization
    """
    if isinstance(value, Decimal):
        return int(value)
    return value


def lambda_handler(event, context):
    # Parse body
    body = json.loads(event.get("body", "{}"))

    # Required keys
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")

    if not all([session_id, project_id, campaign_id]):
        return build_response(400, {"error": "Missing required fields"})

    # Get user by session_id
    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user_id = user_resp["Items"][0]["id"]

    # Fetch existing campaign
    campaign_resp = campaigns_table.get_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        }
    )

    if "Item" not in campaign_resp:
        return build_response(404, {"error": "Campaign not found"})

    campaign = campaign_resp["Item"]

    # Existing values (fallback to DB)
    total_posts = body.get("total_posts", campaign.get("total_posts"))
    posts_per_week = body.get("posts_per_week", campaign.get("posts_per_week"))
    campaign_duration_days = body.get(
        "campaign_duration_days",
        campaign.get("campaign_duration_days")
    )

    # -------- Vice-Versa Calculation Logic --------
    if body.get("total_posts") is not None and posts_per_week:
        weeks = math.ceil(total_posts / posts_per_week)
        campaign_duration_days = weeks * 7

    elif body.get("posts_per_week") is not None and total_posts:
        weeks = math.ceil(total_posts / posts_per_week)
        campaign_duration_days = weeks * 7

    elif body.get("campaign_duration_days") is not None and total_posts:
        weeks = max(1, campaign_duration_days // 7)
        posts_per_week = math.ceil(total_posts / weeks)
        campaign_duration_days = weeks * 7

    # Optional fields to update
    optional_fields = {
        "campaign_name": body.get("campaign_name"),
        "creative_brief": body.get("creative_brief"),
        "key_message": body.get("key_message"),
        "total_posts": total_posts,
        "posts_per_week": posts_per_week,
        "campaign_duration_days": campaign_duration_days,
    }

    # Build DynamoDB UpdateExpression
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

    update_expr = "SET " + ", ".join(expr_parts)

    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        },
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_attr_values
    )

    # Build JSON-safe updated_values for response
    updated_values = {}
    for k, v in optional_fields.items():
        if v is not None:
            if k in ["total_posts", "posts_per_week", "campaign_duration_days"]:
                updated_values[k] = normalize_number(v)
            else:
                updated_values[k] = v

    # Return full response
    return build_response(
        200,
        {
            "message": "Campaign updated successfully",
            "campaign_id": campaign_id,
            "project_id": project_id,
            "user_id": user_id,
            "updated_values": updated_values
        }
    )


def build_response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
