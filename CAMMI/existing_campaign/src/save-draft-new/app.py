import json
import boto3
import base64
import uuid
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# ---------- Tables ----------
POSTS_TABLE = "posts-table"
table = dynamodb.Table(POSTS_TABLE)

# ---------- S3 Bucket ----------
S3_BUCKET = "cammi-devprod"  # <-- replace with your S3 bucket name

# ---------- Timezone ----------
PKT = timezone(timedelta(hours=5))  # Pakistan Time +05:00


def lambda_handler(event, context):
    try:
        print("EVENT RECEIVED:", json.dumps(event))

        # CORS preflight
        if event.get("httpMethod") == "OPTIONS":
            return _response(200, {"message": "CORS preflight"})

        # Parse body
        body = event.get("body")
        if body and isinstance(body, str):
            body = json.loads(body)
        elif not body:
            body = event

        # Mandatory fields
        post_id = body.get("post_id")
        scheduled_time_input = body.get("scheduled_time")

        if not post_id:
            return _response(400, {"error": "post_id is required"})
        if not scheduled_time_input:
            return _response(400, {"error": "scheduled_time is required"})

        # Optional fields: if not provided, set to None (will be null in DynamoDB)
        title = body.get("title") if "title" in body else None
        description = body.get("description") if "description" in body else None
        hashtags = body.get("hashtags") if "hashtags" in body else None
        images = body.get("images") if "images" in body else None

        # ---------- Step 1: Query the post ----------
        response = table.query(
            KeyConditionExpression=Key("post_id").eq(post_id),
            Limit=1
        )
        items = response.get("Items", [])
        if not items:
            return _response(404, {"message": "Post not found", "post_id": post_id})

        post = items[0]
        campaign_id = post["campaign_id"]

        # ---------- Step 2: Handle scheduled_time ----------
        scheduled_dt = datetime.fromisoformat(scheduled_time_input)
        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=PKT)
        scheduled_time_str = scheduled_dt.isoformat()

        # ---------- Step 3: Handle images ----------
        image_keys = []
        if images:
            for idx, img_b64 in enumerate(images):
                img_data = base64.b64decode(img_b64)
                if "," in img_b64:
                    img_b64 = img_b64.split(",")[1]

                img_data = base64.b64decode(img_b64)
                filename = f"images/{post_id}_{uuid.uuid4().hex}_{idx}.jpg"
                s3.put_object(Bucket=S3_BUCKET, Key=filename, Body=img_data, ContentType="image/jpeg")
                image_keys.append(filename)
        else:
            image_keys = None  # explicitly set null if no images

        # ---------- Step 4: Build update expression ----------
        update_expr = (
            "SET #s = :draft, scheduled_time = :scheduled, "
            "#t = :title, description = :desc, hashtags = :tags, image_keys = :images"
        )

        expr_attr_values = {
            ":draft": "draft",
            ":scheduled": scheduled_time_str,
            ":title": title,
            ":desc": description,
            ":tags": hashtags,
            ":images": image_keys
        }

        expr_attr_names = {
            "#s": "status",  # reserved keyword workaround
            "#t": "title"
        }

        # ---------- Step 5: Update DynamoDB ----------
        table.update_item(
            Key={"post_id": post_id, "campaign_id": campaign_id},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values
        )

        return _response(200, {
            "message": "Post updated successfully and set to draft",
            "post_id": post_id
        })

    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON in request body"})

    except ClientError as e:
        return _response(500, {
            "error": "DynamoDB/S3 operation failed",
            "details": e.response["Error"]["Message"]
        })

    except Exception as e:
        return _response(500, {"error": "Internal server error", "details": str(e)})


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
