import json
import uuid
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("posts-table")


def lambda_handler(event, context):
    try:
        print("EVENT RECEIVED:", json.dumps(event))

        # CORS
        if event.get("httpMethod") == "OPTIONS":
            return _response(200, {"message": "CORS preflight"})

        body = event.get("body")
        if body and isinstance(body, str):
            body = json.loads(body)
        elif not body:
            body = event

        campaign_id = body.get("campaign_id")
        scheduled_time = body.get("scheduled_time")
        description = body.get("description")

        if not campaign_id or not scheduled_time or not description:
            return _response(
                400,
                {"error": "campaign_id, scheduled_time and description are required"}
            )

        # ✅ 12-char hex post_id
        post_id = uuid.uuid4().hex[:12]

        item = {
            "post_id": post_id,
            "campaign_id": campaign_id,
            "description": description,
            "scheduled_time": scheduled_time,
            "status": "Generated",
            "created_at": datetime.utcnow().isoformat()
        }

        # Optional fields
        if "title" in body:
            item["title"] = body["title"]
        if "hashtags" in body:
            item["hashtags"] = body["hashtags"]
        if "image_generation_prompt" in body:
            item["image_generation_prompt"] = body["image_generation_prompt"]

        table.put_item(Item=item)

        return _response(
            200,
            {
                "message": "Post generated successfully",
                "post_id": post_id,
                "campaign_id": campaign_id,
                "status": "Generated"
            }
        )

    except ClientError as e:
        return _response(
            500,
            {
                "error": "DynamoDB operation failed",
                "details": e.response["Error"]["Message"]
            }
        )

    except Exception as e:
        return _response(
            500,
            {
                "error": "Internal server error",
                "details": str(e)
            }
        )


def _response(status_code, body):
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
