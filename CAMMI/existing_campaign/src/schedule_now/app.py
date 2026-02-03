import json
import boto3
from datetime import datetime, timedelta, timezone

dynamodb = boto3.resource("dynamodb")
scheduler = boto3.client("scheduler")  # EventBridge Scheduler client

POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"

STATUS_LAMBDA_ARN = "arn:aws:lambda:us-east-1:687088702813:function:status"
EVENTBRIDGE_ROLE_ARN = "arn:aws:iam::687088702813:role/scheduler-invoke-lambda-role"
S3_BUCKET = "cammi-devprod"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)


def lambda_handler(event, context):
    try:
        # -------------------------------
        # Handle CORS preflight
        # -------------------------------
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        # -------------------------------
        # 1. Extract input from API event
        # -------------------------------
        body = json.loads(event.get("body", "{}"))
        post_id = body.get("post_id")
        sub = body.get("sub")

        if not post_id or not sub:
            return response(400, "post_id and sub are required")

        # -----------------------------------------
        # 2. Query posts-table using post_id
        # -----------------------------------------
        query_resp = posts_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("post_id").eq(post_id)
        )

        if not query_resp["Items"]:
            return response(404, "Post not found")

        post_item = query_resp["Items"][0]

        campaign_id = post_item["campaign_id"]
        image_keys = post_item.get("image_keys", [])
        title = post_item.get("title", "")
        description = post_item.get("description", "")
        hashtag = post_item.get("hashtag", "")
        scheduled_time_str = post_item.get("scheduled_time")

        # -----------------------------------------
        # 3. Build message
        # -----------------------------------------
        message = f"{title}\n\n{description}\n\n{hashtag}"

        # -----------------------------------------
        # 4. Add 2 minutes to scheduled_time
        #    and convert to UTC+5 for DynamoDB
        # -----------------------------------------
        scheduled_time = datetime.fromisoformat(scheduled_time_str)
        updated_scheduled_time = scheduled_time + timedelta(minutes=2)

        # Convert to UTC+5 for DynamoDB
        PKT = timezone(timedelta(hours=5))
        updated_scheduled_time_pkt = updated_scheduled_time.astimezone(PKT)
        updated_scheduled_time_pkt_str = updated_scheduled_time_pkt.isoformat()

        # Convert to UTC for EventBridge Scheduler
        updated_scheduled_time_utc = updated_scheduled_time_pkt.astimezone(timezone.utc)
        utc_str = updated_scheduled_time_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        # -----------------------------------------
        # 5. Update posts-table
        # -----------------------------------------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="""
                SET scheduled_time = :st,
                    #status = :status
            """,
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":st": updated_scheduled_time_pkt_str,
                ":status": "scheduled"
            }
        )

        # -----------------------------------------
        # 6. Update linkedin-posts-table
        # -----------------------------------------
        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_time": updated_scheduled_time_pkt_str,
                "post_id": post_id,
                "campaign_id": campaign_id,
                "message": message,
                "image_keys": image_keys,
                "scheduled_time": updated_scheduled_time_pkt_str,
                "status": "scheduled"
            }
        )

        # -----------------------------------------
        # 7. Create EventBridge schedule
        # -----------------------------------------
        schedule_name = f"linkedin_post_{sub}_{int(datetime.now().timestamp())}"

        scheduler.create_schedule(
            Name=schedule_name,
            GroupName="default",
            FlexibleTimeWindow={"Mode": "OFF"},
            ScheduleExpression=f"at({utc_str})",
            Target={
                "Arn": STATUS_LAMBDA_ARN,
                "RoleArn": EVENTBRIDGE_ROLE_ARN,
                "Input": json.dumps(post_item)  # Send original post_item
            }
        )

        # -----------------------------------------
        # 8. Return response with schedule_name
        # -----------------------------------------
        return response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time": updated_scheduled_time_pkt_str,
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
