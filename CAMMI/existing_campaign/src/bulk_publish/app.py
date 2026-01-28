import json
import boto3
from boto3.dynamodb.conditions import Key

# DynamoDB resources
dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("posts-table")
linkedin_posts_table = dynamodb.Table("linkedin-posts-table")


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

    # Query posts_table using the GSI
    response = posts_table.query(
        IndexName="campaign_id-index",
        KeyConditionExpression=Key("campaign_id").eq(campaign_id)
    )

    posts = response.get("Items", [])
    print("Number of posts returned by DynamoDB:", len(posts))

    if not posts:
        return _cors_response(200, {"message": "No posts found for this campaign"})

    scheduled_count = 0

    for post in posts:
        scheduled_time = post.get("scheduled_time")
        if not scheduled_time:
            continue

        title = post.get("title", "")
        description = post.get("description", "")

        hashtags_list = post.get("hashtags", [])
        hashtags = " ".join(
            h.get("S") if isinstance(h, dict) and "S" in h else str(h)
            for h in hashtags_list
        )

        message = f"{title}\n\n{description}\n\n{hashtags}".strip()

        # Prepare item for linkedin-posts-table
        item = {
            "sub": sub,
            "post_time": scheduled_time,      # Sort Key
            "scheduled_time": scheduled_time,
            "message": message,
            "status": "scheduled"
        }

        if post.get("image_keys"):
            item["image_keys"] = post["image_keys"]

        # Insert into linkedin-posts-table
        linkedin_posts_table.put_item(Item=item)

        # 🔥 ONLY NEW CHANGE: update status in posts-table
        posts_table.update_item(
            Key={
                "post_id": post["post_id"],   # partition key
                "campaign_id": post["campaign_id"]  # sort key (if applicable)
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
