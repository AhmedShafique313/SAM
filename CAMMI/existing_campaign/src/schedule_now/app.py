import json
import boto3
from datetime import datetime, timezone, timedelta
from boto3.dynamodb.conditions import Key

# ---------------- AWS Clients ----------------
dynamodb = boto3.resource("dynamodb")
scheduler = boto3.client("scheduler")

# ---------------- Constants ----------------
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"

PKT = timezone(timedelta(hours=5))

# ---------------- Tables ----------------
posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)


def lambda_handler(event, context):
    try:
        # -------------------------------
        # CORS
        # -------------------------------
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        # -------------------------------
        # 1. Parse input
        # -------------------------------
        body = json.loads(event.get("body", "{}"))
        post_id = body.get("post_id")
        sub = body.get("sub")

        if not post_id or not sub:
            return response(400, "post_id and sub are required")

        # -------------------------------
        # 2. Fetch post from posts-table
        # -------------------------------
        post_resp = posts_table.query(
            KeyConditionExpression=Key("post_id").eq(post_id)
        )

        if not post_resp["Items"]:
            return response(404, "Post not found")

        post_item = post_resp["Items"][0]

        campaign_id = post_item["campaign_id"]
        title = post_item.get("title", "")
        description = post_item.get("description", "")
        hashtag = post_item.get("hashtag", "")
        image_keys = post_item.get("image_keys", [])
        scheduled_time_str = post_item.get("scheduled_time")

        if not scheduled_time_str:
            return response(400, "scheduled_time missing in post")

        # -------------------------------
        # 3. Build LinkedIn message
        # -------------------------------
        message = f"{title}\n\n{description}\n\n{hashtag}"

        # -------------------------------
        # 4. Time handling (STRICT)
        # -------------------------------
        pkt_time = datetime.fromisoformat(scheduled_time_str).astimezone(PKT)
        pkt_time_str = pkt_time.isoformat()

        utc_time = pkt_time.astimezone(timezone.utc)
        utc_str = utc_time.strftime("%Y-%m-%dT%H:%M:%S")  # NO Z

        # -------------------------------
        # 5. Create EventBridge schedule
        # -------------------------------
        schedule_name = f"linkedin_post_{sub}_{int(datetime.utcnow().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                # 🔑 EXACT PAYLOAD EXPECTED BY status LAMBDA
                "Input": json.dumps({
                    "sub": sub,
                    "message": message,
                    "scheduled_time": pkt_time_str,
                    "image_keys": image_keys
                })
            }
        )

        # -------------------------------
        # 6. Update posts-table
        # -------------------------------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="SET scheduled_time = :st, #status = :s",
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":st": pkt_time_str,
                ":s": "scheduled"
            }
        )

        # -------------------------------
        # 7. Insert linkedin-posts-table
        # -------------------------------
        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_time": pkt_time_str,      # 🔑 SORT KEY MATCHES status lambda
                "post_id": post_id,
                "campaign_id": campaign_id,
                "message": message,
                "image_keys": image_keys,
                "scheduled_time": pkt_time_str,
                "status": "scheduled"
            }
        )

        # -------------------------------
        # 8. Success
        # -------------------------------
        return response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time": pkt_time_str,
            "scheduled_time_utc": utc_str,
            "schedule_name": schedule_name
        })

    except scheduler.exceptions.ValidationException as e:
        return response(400, f"Invalid schedule time: {str(e)}")

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
