import json
import boto3
import base64
import re
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ----------------------------
# DynamoDB
# ----------------------------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("posts-table")


# ----------------------------
# Base64 Sanitizer (HARD FIX)
# ----------------------------
def sanitize_base64(b64_string: str) -> str:
    if not isinstance(b64_string, str):
        return b64_string

    # Remove data URI prefix
    b64_string = re.sub(r"^data:image\/[a-zA-Z]+;base64,", "", b64_string)

    # Normalize whitespace
    b64_string = b64_string.strip().replace("\n", "").replace("\r", "")

    # Normalize URL-safe base64
    b64_string = b64_string.replace("-", "+").replace("_", "/")

    # Remove ALL invalid base64 characters
    b64_string = re.sub(r"[^A-Za-z0-9+/=]", "", b64_string)

    # Fix padding safely
    padding = len(b64_string) % 4
    if padding:
        b64_string += "=" * (4 - padding)

    return b64_string


def sanitize_images(payload: dict):
    images = payload.get("images")

    if not images:
        return

    # Single image
    if isinstance(images, str):
        payload["images"] = sanitize_base64(images)
        return

    # Multiple images
    if isinstance(images, list):
        payload["images"] = [sanitize_base64(img) for img in images]


# ----------------------------
# Lambda Handler
# ----------------------------
def lambda_handler(event, context):
    try:
        print("EVENT RECEIVED")

        # CORS
        if event.get("httpMethod") == "OPTIONS":
            return _response(200, {"message": "CORS preflight"})

        body = event.get("body")
        if body and isinstance(body, str):
            body = json.loads(body)
        elif not body:
            body = event

        # 🔥 FINAL FIX
        sanitize_images(body)

        post_id = body.get("post_id")
        if not post_id:
            return _response(400, {"error": "post_id is required"})

        response = table.query(
            KeyConditionExpression=Key("post_id").eq(post_id),
            Limit=1
        )

        items = response.get("Items", [])
        if not items:
            return _response(404, {"message": "Post not found", "post_id": post_id})

        post = items[0]
        campaign_id = post["campaign_id"]

        table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="SET #s = :draft",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":draft": "draft"}
        )

        return _response(200, {
            "message": "Post status updated to draft",
            "post_id": post_id
        })

    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON in request body"})

    except ClientError as e:
        return _response(500, {
            "error": "DynamoDB operation failed",
            "details": e.response["Error"]["Message"]
        })

    except Exception as e:
        return _response(500, {
            "error": "Internal server error",
            "details": str(e)
        })


# ----------------------------
# Response Helper
# ----------------------------
def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,PUT,GET,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
