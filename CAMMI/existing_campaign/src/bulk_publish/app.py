import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone, timedelta
import time

# ---------- Timezone ----------
PKT = timezone(timedelta(hours=5))  # Pakistan Time UTC+5

# ---------- AWS Resources ----------
dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("posts-table")
linkedin_posts_table = dynamodb.Table("linkedin-posts-table")
scheduler = boto3.client("scheduler")

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"

# ---------- Lambda Handler ----------
def lambda_handler(event, context):

    # ---------- CORS ----------
    if event.get("httpMethod") == "OPTIONS":
        return _cors_response(200, {})

    body = json.loads(event.get("body") or "{}")
    sub = body.get("sub")
    campaign_id = body.get("campaign_id")

    if not sub or not campaign_id:
        return _cors_response(400, {"error": "sub and campaign_id are required"})

    campaign_id = str(campaign_id).strip()

    # ---------- Query Posts ----------
    response = posts_table.query(
        IndexName="campaign_id-index",
        KeyConditionExpression=Key("campaign_id").eq(campaign_id),
        FilterExpression=Attr("status").eq("Generated")
    )

    posts = response.get("Items", [])
    if not posts:
        return _cors_response(200, {
            "message": "No posts with status 'Generated' found"
        })

    scheduled_count = 0

    for post in posts:

        scheduled_time = post.get("scheduled_time")
        if not scheduled_time:
            continue

        # ---------- Parse PKT time ----------
        scheduled_dt = datetime.fromisoformat(scheduled_time)

        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=PKT)

        # ---------- Convert to UTC (NO Z) ----------
        utc_time = scheduled_dt.astimezone(timezone.utc)
        utc_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")

        # ---------- Store PKT for DB ----------
        scheduled_pkt_str = scheduled_dt.astimezone(PKT).strftime(
            "%Y-%m-%dT%H:%M:%S+05:00"
        )

        post_id = post.get("post_id")
        title = post.get("title", "")
        description = post.get("description", "")

        hashtags_list = post.get("hashtags", [])
        hashtags = " ".join(
            h.get("S") if isinstance(h, dict) else str(h)
            for h in hashtags_list
        )

        message = f"{title}\n\n{description}\n\n{hashtags}".strip()

        image_keys = post.get("image_keys", [])

        # ---------- Insert into linkedin-posts-table ----------
        linkedin_posts_table.put_item(
            Item={
                "sub": sub,
                "post_id": post_id,
                "campaign_id": campaign_id,
                "scheduled_time": scheduled_pkt_str,
                "post_time": scheduled_pkt_str,
                "message": message,
                "image_keys": image_keys,
                "status": "scheduled"
            }
        )

        # ---------- EventBridge Schedule ----------
        schedule_name = f"linkedin_post_{sub}_{post_id}_{int(datetime.utcnow().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps({
                    "sub": sub,
                    "message": message,
                    "scheduled_time": scheduled_pkt_str,
                    "image_keys": image_keys
                })
            }
        )

        # ---------- Update status ----------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="SET #st = :s",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={":s": "scheduled"}
        )

        scheduled_count += 1

    return _cors_response(200, {
        "message": "All posts scheduled successfully",
        "scheduled_posts": scheduled_count
    })


# ---------- CORS Helper ----------
def _cors_response(status_code, body):
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
