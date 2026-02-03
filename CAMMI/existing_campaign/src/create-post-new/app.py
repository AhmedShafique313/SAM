import json
import boto3
import uuid
from datetime import datetime, timezone

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")

# ---------- Table ----------
POSTS_TABLE = "posts-table"
posts_table = dynamodb.Table(POSTS_TABLE)


def lambda_handler(event, context):
    try:
        # ---------- CORS ----------
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        # ---------- Input ----------
        body = json.loads(event.get("body", "{}"))
        campaign_id = body.get("campaign_id")

        if not campaign_id:
            return response(400, "campaign_id is required")

        # ---------- Generate post_id ----------
        post_id = uuid.uuid4().hex  # e.g. 7e29f6c38c5a...

        # ---------- Create dummy post ----------
        item = {
            "post_id": post_id,
            "campaign_id": campaign_id,

            "title": "Untitled",
            "description": None,

            "best_post_day": None,
            "best_post_time": None,

            "generated_at": None,
            "scheduled_time": None,

            "hashtag": None,
            "hashtags": [],

            "image_generation_prompt": None,
            "image_keys": None,

            "status": "draft"
        }

        posts_table.put_item(Item=item)

        # ---------- Response ----------
        return response(200, {
            "message": "Draft post created successfully",
            "post_id": post_id,
            "campaign_id": campaign_id
        })

    except Exception as e:
        return response(500, str(e))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
