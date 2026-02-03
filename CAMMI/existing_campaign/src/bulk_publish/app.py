import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime, timezone, timedelta

# Define Pakistan Time (PKT) UTC+5
PKT = timezone(timedelta(hours=5))

# DynamoDB resources
dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("posts-table")
linkedin_posts_table = dynamodb.Table("linkedin-posts-table")

# EventBridge Scheduler
scheduler = boto3.client("scheduler")

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"


def lambda_handler(event, context):
    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return _cors_response(200, {})

    # Parse body
    body = json.loads(event.get("body") or "{}")
    sub = body.get("sub")
    campaign_id = body.get("campaign_id")

    if not sub or not campaign_id:
        return _cors_response(400, {"error": "sub and campaign_id are required"})

    campaign_id = str(campaign_id).strip()

    print("Querying posts for campaign_id:", repr(campaign_id))

    # Query posts_table using the GSI and filter only posts with status = Generated
    response = posts_table.query(
        IndexName="campaign_id-index",
        KeyConditionExpression=Key("campaign_id").eq(campaign_id),
        FilterExpression=Attr("status").eq("Generated")  # Only Generated posts
    )

    posts = response.get("Items", [])
    print("Number of posts returned by DynamoDB:", len(posts))

    if not posts:
        return _cors_response(200, {"message": "No posts with status 'Generated' found for this campaign"})

    scheduled_count = 0

    for post in posts:
        scheduled_time = post.get("scheduled_time")
        if not scheduled_time:
            continue

        # Convert scheduled_time to datetime and set PKT timezone if missing
        scheduled_dt = datetime.fromisoformat(scheduled_time)
        if scheduled_dt.tzinfo is None:
            scheduled_dt = scheduled_dt.replace(tzinfo=PKT)

        # Force PKT format string
        scheduled_time_str = scheduled_dt.strftime("%Y-%m-%dT%H:%M:%S+05:00")  # Always +05:00

        post_id = post.get("post_id")
        campaign_id = post.get("campaign_id")

        title = post.get("title", "")
        description = post.get("description", "")

        hashtags_list = post.get("hashtags", [])
        hashtags = " ".join(
            h.get("S") if isinstance(h, dict) and "S" in h else str(h)
            for h in hashtags_list
        )

        message = f"{title}\n\n{description}\n\n{hashtags}".strip()

        # Prepare item for linkedin-posts-table
        post_item = {
            "sub": sub,
            "post_time": scheduled_time_str,      # PKT +05:00
            "post_id": post_id,
            "campaign_id": campaign_id,
            "scheduled_time": scheduled_time_str, # PKT +05:00
            "message": message,
            "status": "scheduled"
        }

        if post.get("image_keys"):
            post_item["image_keys"] = post["image_keys"]

        # Insert into linkedin-posts-table
        linkedin_posts_table.put_item(Item=post_item)

        # Create EventBridge schedule for this post
        utc_str = scheduled_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")  # convert to UTC for schedule
        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps(post_item)   # send the same post item
            }
        )

        # Update status in posts-table to "scheduled"
        posts_table.update_item(
            Key={
                "post_id": post_id,        # partition key
                "campaign_id": campaign_id # sort key
            },
            UpdateExpression="SET #st = :scheduled",
            ExpressionAttributeNames={
                "#st": "status"
            },
            ExpressionAttributeValues={
                ":scheduled": "scheduled"
            }
        )

        scheduled_count += 1

    return _cors_response(
        200,
        {
            "message": "Posts scheduled successfully",
            "scheduled_posts": scheduled_count
        }
    )


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
