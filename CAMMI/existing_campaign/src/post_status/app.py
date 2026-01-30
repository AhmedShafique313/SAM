import json
import boto3
import base64
from boto3.dynamodb.conditions import Key

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# ---------- Tables ----------
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"

POSTS_CAMPAIGN_GSI = "campaign_id-index"
LINKEDIN_CAMPAIGN_GSI = "campaign_id-index"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)

# ---------- Default S3 Bucket (used if bucket not in key) ----------
DEFAULT_BUCKET = "cammi-devprod"


def download_and_base64(image_key: str):
    """
    image_key formats supported:
    - bucket-name/path/to/image.png
    - path/to/image.png (uses DEFAULT_BUCKET)
    """
    try:
        if "/" in image_key and image_key.split("/")[0].count("-") >= 1:
            bucket, key = image_key.split("/", 1)
        else:
            bucket = DEFAULT_BUCKET
            key = image_key

        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read()

        return base64.b64encode(content).decode("utf-8")

    except Exception:
        return None


def attach_images(items):
    """
    Adds image_base64 field if image_keys exist
    """
    for item in items:
        image_keys = item.get("image_keys")

        if image_keys and isinstance(image_keys, list):
            base64_images = []

            for key in image_keys:
                encoded = download_and_base64(key)
                if encoded:
                    base64_images.append(encoded)

            if base64_images:
                item["image_base64"] = base64_images

    return items


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        campaign_id = body.get("campaign_id")

        if not campaign_id:
            return response(400, {"error": "campaign_id is required"})

        posts_result = posts_table.query(
            IndexName=POSTS_CAMPAIGN_GSI,
            KeyConditionExpression=Key("campaign_id").eq(campaign_id)
        )

        draft_posts = [
            item for item in posts_result.get("Items", [])
            if item.get("status") == "draft"
        ]

        draft_posts = attach_images(draft_posts)

        linkedin_result = linkedin_table.query(
            IndexName=LINKEDIN_CAMPAIGN_GSI,
            KeyConditionExpression=Key("campaign_id").eq(campaign_id)
        )

        linkedin_posts = [
            item for item in linkedin_result.get("Items", [])
            if item.get("status") in ["scheduled", "published"]
        ]

        linkedin_posts = attach_images(linkedin_posts)

        # ✅ ONLY CHANGE: merge both into a single posts array
        all_posts = draft_posts + linkedin_posts

        return response(200, {
            "campaign_id": campaign_id,
            "posts": all_posts
        })

    except Exception as e:
        return response(500, {
            "error": "Internal Server Error",
            "details": str(e)
        })


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body)
    }
