import json
import boto3
import base64
from datetime import datetime, timezone, timedelta

scheduler = boto3.client("scheduler")
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

post_table = dynamodb.Table("linkedin-posts-table")
user_table = dynamodb.Table("linkedin-user-table")

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"
S3_BUCKET = "cammi-devprod"

PKT = timezone(timedelta(hours=5))


def lambda_handler(event, context):
    try:
        if event.get("httpMethod") == "OPTIONS":
            return _resp(200, {})

        body = json.loads(event.get("body", "{}"))

        sub = body.get("sub")
        message = body.get("message")
        scheduled_time = body.get("scheduled_time")  # ISO string PKT
        images = body.get("images", [])

        if not sub or not message or not scheduled_time:
            return _resp(400, {"error": "sub, message, scheduled_time required"})

        # Validate user
        user = user_table.get_item(Key={"sub": sub}).get("Item")
        if not user or not user.get("access_token"):
            return _resp(400, {"error": "Invalid user"})

        # Upload images to S3
        image_keys = []
        for img in images:
            image_b64 = img.get("image")
            filename = img.get("filename", "upload.jpg")

            if not image_b64:
                continue

            if "," in image_b64:
                image_b64 = image_b64.split(",", 1)[1]

            image_bytes = base64.b64decode(image_b64)
            key = f"{sub}/{filename}"

            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=image_bytes,
                ContentType="image/jpeg"
            )

            image_keys.append(key)

        # ✅ SAVE EXACT SORT KEY
        post_item = {
            "sub": sub,
            "post_time": scheduled_time,     # 🔑 NEVER CHANGE THIS
            "scheduled_time": scheduled_time,
            "message": message,
            "status": "scheduled",
        }

        if image_keys:
            post_item["image_keys"] = image_keys

        post_table.put_item(Item=post_item)

        # Convert PKT → UTC
        scheduled_dt = datetime.fromisoformat(scheduled_time)
        utc_dt = scheduled_dt.astimezone(timezone.utc)
        utc_str = utc_dt.strftime("%Y-%m-%dT%H:%M:%S")

        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps(post_item)   # 🔑 same object
            }
        )

        return _resp(200, {
            "message": "Post scheduled",
            "scheduled_time_pkt": scheduled_time,
            "scheduled_time_utc": utc_str,
            "schedule_name": schedule_name
        })

    except Exception as e:
        return _resp(500, {"error": str(e)})


def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
