import json
import boto3
import base64
import uuid
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key

# ---------- AWS clients
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
scheduler = boto3.client("scheduler")

# ---------- Constants
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"
S3_BUCKET = "cammi-devprod"

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"

PKT = timezone(timedelta(hours=5))

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)


def lambda_handler(event, context):
    try:
        # ---------- CORS
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        # ---------- Robust body parsing (works for Postman, Lambda test, API Gateway)
        if "body" in event:
            body = event["body"]
            if isinstance(body, str):
                body = json.loads(body)
        else:
            body = event

        # ---------- Required fields
        post_id = body.get("post_id")
        sub = body.get("sub")
        scheduled_time_input = body.get("scheduled_time")

        if not post_id or not sub or not scheduled_time_input:
            return response(400, "post_id, sub, and scheduled_time are required")

        # ---------- Optional fields
        title = body.get("title")
        description = body.get("description")
        hashtags = body.get("hashtags") or body.get("hashtag")
        images = body.get("images")

        # ---------- scheduled_time (frontend is source of truth)
        scheduled_dt = datetime.fromisoformat(scheduled_time_input)
        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=PKT)

        scheduled_time_str = scheduled_dt.isoformat()

        # ---------- Fetch post
        query_resp = posts_table.query(
            KeyConditionExpression=Key("post_id").eq(post_id)
        )
        if not query_resp["Items"]:
            return response(404, "Post not found")

        post_item = query_resp["Items"][0]
        campaign_id = post_item["campaign_id"]

        # ---------- Image handling
        image_keys = post_item.get("image_keys", [])
        if images is not None:
            image_keys = []
            for img in images:
                if "," in img:
                    img = img.split(",", 1)[1]  # remove data:image/...;base64,
                image_bytes = base64.b64decode(img)
                key = f"images/{uuid.uuid4().hex}.jpg"
                s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=key,
                    Body=image_bytes,
                    ContentType="image/jpeg"
                )
                image_keys.append(key)

        # ---------- Update posts-table
        posts_table.update_item(
            Key={"post_id": post_id, "campaign_id": campaign_id},
            UpdateExpression="""
                SET scheduled_time = :st,
                    #status = :status,
                    title = :title,
                    description = :desc,
                    hashtags = :tags,
                    image_keys = :imgs
            """,
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":st": scheduled_time_str,
                ":status": "scheduled",
                ":title": title,
                ":desc": description,
                ":tags": hashtags,
                ":imgs": image_keys
            }
        )

        # ---------- linkedin-posts-table
        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_id": post_id,
                "campaign_id": campaign_id,
                "scheduled_time": scheduled_time_str,
                "post_time": scheduled_time_str,
                "message": f"{title or ''}\n\n{description or ''}\n\n{hashtags or ''}",
                "image_keys": image_keys,
                "status": "scheduled"
            }
        )

        # ---------- EventBridge Scheduler (UTC exact frontend time)
        utc_time = scheduled_dt.astimezone(timezone.utc)
        utc_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")

        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps({
                    "post_id": post_id,
                    "campaign_id": campaign_id,
                    "scheduled_time": scheduled_time_str,
                    "image_keys": image_keys,
                    "status": "scheduled"
                })
            }
        )

        return response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time": scheduled_time_str,
            "schedule_name": schedule_name
        })

    except Exception as e:
        return response(500, str(e))


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(body)
    }
