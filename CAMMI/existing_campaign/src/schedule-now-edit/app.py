import json
import boto3
import base64
import uuid
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
scheduler = boto3.client("scheduler")

# ---------- Tables & Bucket ----------
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"
S3_BUCKET = "cammi-devprod"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)

# ---------- Lambda & Role ARNs ----------
STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"

# ---------- Timezone (PKT / UTC+5) ----------
PKT = timezone(timedelta(hours=5))


def lambda_handler(event, context):
    try:
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        body = json.loads(event.get("body", "{}"))

        post_id = body.get("post_id")
        sub = body.get("sub")
        scheduled_time_input = body.get("scheduled_time")

        if not post_id or not sub or not scheduled_time_input:
            return response(400, "post_id, sub, and scheduled_time are required")

        title = body.get("title")
        description = body.get("description")
        hashtag = body.get("hashtag")
        images = body.get("images", [])

        # -----------------------------------------
        # Normalize scheduled_time (PKT)
        # -----------------------------------------
        scheduled_dt = datetime.fromisoformat(scheduled_time_input)

        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=PKT)

        scheduled_time_str = scheduled_dt.isoformat()

        # -----------------------------------------
        # Fetch post
        # -----------------------------------------
        query_resp = posts_table.query(
            KeyConditionExpression=Key("post_id").eq(post_id)
        )

        if not query_resp["Items"]:
            return response(404, "Post not found")

        post_item = query_resp["Items"][0]
        campaign_id = post_item["campaign_id"]

        # -----------------------------------------
        # Handle images (optional)
        # -----------------------------------------
        image_keys = None
        if images:
            image_keys = []
            for img_base64 in images:
                image_bytes = base64.b64decode(img_base64)
                image_name = f"images/{uuid.uuid4().hex}.jpg"

                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=image_name,
                    Body=image_bytes,
                    ContentType="image/jpeg"
                )

                image_keys.append(image_name)

        # -----------------------------------------
        # Update posts-table
        # -----------------------------------------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="""
                SET scheduled_time = :st,
                    #status = :status,
                    title = :title,
                    description = :desc,
                    hashtag = :tag,
                    image_keys = :imgs
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":st": scheduled_time_str,
                ":status": "scheduled",
                ":title": title,
                ":desc": description,
                ":tag": hashtag,
                ":imgs": image_keys
            }
        )

        # -----------------------------------------
        # LinkedIn table
        # -----------------------------------------
        message = f"{title or ''}\n\n{description or ''}\n\n{hashtag or ''}"

        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_time": scheduled_time_str,
                "post_id": post_id,
                "campaign_id": campaign_id,
                "message": message,
                "image_keys": image_keys,
                "scheduled_time": scheduled_time_str,
                "status": "scheduled"
            }
        )

        # -----------------------------------------
        # EventBridge Scheduler (UTC, NO TZ)
        # -----------------------------------------
        scheduled_utc = scheduled_dt.astimezone(timezone.utc)

        # ✅ ONLY FORMAT THAT WORKS
        utc_str = scheduled_utc.strftime("%Y-%m-%dT%H:%M:%S")

        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps(post_item)
            }
        )

        return response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time": scheduled_time_str,
            "schedule_name": schedule_name
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
