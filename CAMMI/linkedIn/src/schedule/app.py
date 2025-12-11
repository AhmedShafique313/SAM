import json
import boto3
import base64
from datetime import datetime, timezone, timedelta

# AWS clients
scheduler = boto3.client("scheduler")
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

post_table = dynamodb.Table("linkedin-posts-table")
user_table = dynamodb.Table("linkedin-user-table")

# Config
STATUS_TEXT_POST_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"
S3_BUCKET = "cammi-devprod"

# Pakistan Standard Time
PKT = timezone(timedelta(hours=5))

def lambda_handler(event, context):
    try:
        # Handle CORS preflight request
        if event.get("httpMethod") == "OPTIONS":
            return _cors_response(200, {})

        body = event.get("body")
        if body:
            body = json.loads(body) if isinstance(body, str) else body
        else:
            return _cors_response(400, {"error": "Request body required"})

        sub = body.get("sub")
        message = body.get("message")
        scheduled_time = body.get("scheduled_time")  # ISO 8601 string in PKT
        images = body.get("images", [])  # optional list

        if not sub or not message or not scheduled_time:
            return _cors_response(400, {"error": "sub, message, scheduled_time required"})

        # Validate user
        user = user_table.get_item(Key={"sub": sub}).get("Item")
        if not user or not user.get("access_token"):
            return _cors_response(400, {"error": f"Invalid user or missing access token for {sub}"})

        # 🔹 Upload images to S3 and collect only KEYS (not presigned URLs)
        image_keys = []
        for img in images:
            image_b64 = img.get("image")
            filename = img.get("filename", "upload.png")

            if not image_b64:
                continue

            image_bytes = base64.b64decode(image_b64)

            s3_key = f"{sub}/{filename}"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_key,
                Body=image_bytes,
                ContentType="image/png" if filename.lower().endswith("png") else "image/jpeg"
            )

            image_keys.append(s3_key)

        # Save post info in DynamoDB
        post_item = {
            "sub": sub,
            "message": message,
            "post_time": scheduled_time,
            "scheduled_time": scheduled_time,
            "status": "pending"
        }
        if image_keys:
            post_item["image_keys"] = image_keys   # ✅ store KEYS instead of URLs

        post_table.put_item(Item=post_item)

        # Convert PKT to UTC for EventBridge
        scheduled_dt = datetime.fromisoformat(scheduled_time)
        scheduled_dt_utc = scheduled_dt.astimezone(timezone.utc)
        utc_time_str = scheduled_dt_utc.strftime("%Y-%m-%dT%H:%M:%S")

        # Unique schedule name
        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        # Create EventBridge schedule
        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_time_str})",
            Target={
                "Arn": STATUS_TEXT_POST_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps(post_item)
            }
        )

        return _cors_response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time_pkt": scheduled_time,
            "scheduled_time_utc": utc_time_str,
            "schedule_name": schedule_name,
            "image_keys": image_keys  # ✅ return keys for debugging
        })

    except Exception as e:
        return _cors_response(500, {"error": str(e)})


def _cors_response(status_code, body):
    """Always include CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
