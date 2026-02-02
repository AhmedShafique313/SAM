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

# ---------- Default S3 Bucket ----------
DEFAULT_BUCKET = "cammi-devprod"


# ---------- S3 IMAGE HELPERS ----------
def download_and_base64(image_key: str):
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


# ---------- NORMALIZATION ----------
def normalize_linkedin_post(item):
    """
    Converts LinkedIn posts to match draft post schema
    """
    # ---- MESSAGE → TITLE / DESCRIPTION ----
    message = item.get("message", "")
    parts = [p.strip() for p in message.split("\n\n") if p.strip()]

    item["title"] = item.get("title") or (parts[0] if len(parts) > 0 else "")
    item["description"] = item.get("description") or (parts[1] if len(parts) > 1 else "")

    # ---- HASHTAG NORMALIZATION ----
    hashtag_str = item.get("hashtag", "")
    if hashtag_str:
        item["hashtags"] = hashtag_str.split()
    else:
        item["hashtags"] = []

    # ---- CLEANUP ----
    item.pop("message", None)
    item.pop("hashtag", None)

    return item


# ---------- LAMBDA ----------
def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        campaign_id = body.get("campaign_id")

        if not campaign_id:
            return response(400, {"error": "campaign_id is required"})

        # ---------- DRAFT POSTS ----------
        posts_result = posts_table.query(
            IndexName=POSTS_CAMPAIGN_GSI,
            KeyConditionExpression=Key("campaign_id").eq(campaign_id)
        )

        draft_posts = [
            item for item in posts_result.get("Items", [])
            if item.get("status") == "draft"
        ]

        draft_posts = attach_images(draft_posts)

        # ---------- LINKEDIN POSTS ----------
        linkedin_result = linkedin_table.query(
            IndexName=LINKEDIN_CAMPAIGN_GSI,
            KeyConditionExpression=Key("campaign_id").eq(campaign_id)
        )

        linkedin_posts = [
            normalize_linkedin_post(item)
            for item in linkedin_result.get("Items", [])
            if item.get("status") in ["scheduled", "published"]
        ]

        linkedin_posts = attach_images(linkedin_posts)

        # ---------- MERGE ----------
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


# ---------- RESPONSE ----------
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
