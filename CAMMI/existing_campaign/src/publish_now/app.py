import json
import boto3
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")

POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)


def lambda_handler(event, context):
    try:
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
            KeyConditionExpression=Key("post_id").eq(post_id)
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
        # -----------------------------------------
        scheduled_time = datetime.fromisoformat(scheduled_time_str)
        updated_scheduled_time = scheduled_time + timedelta(minutes=2)
        updated_scheduled_time_str = updated_scheduled_time.isoformat()

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
                ":st": updated_scheduled_time_str,
                ":status": "scheduled"
            }
        )

        # -----------------------------------------
        # 6. Update linkedin-posts-table
        # -----------------------------------------
        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_time": updated_scheduled_time_str,
                "message": message,
                "image_keys": image_keys,
                "scheduled_time": updated_scheduled_time_str,
                "status": "scheduled"
            }
        )

        return response(200, {
            "message": "Post scheduled successfully",
            "scheduled_time": updated_scheduled_time_str
        })

    except Exception as e:
        return response(500, str(e))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body)
    }
