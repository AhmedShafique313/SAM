import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("posts-table")


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    # Required fields
    post_id = body.get("post_id")
    campaign_id = body.get("campaign_id")

    if not post_id or not campaign_id:
        return build_response(400, {"error": "post_id and campaign_id are required"})

    # Check if post exists
    post_resp = posts_table.get_item(
        Key={
            "post_id": post_id,
            "campaign_id": campaign_id
        }
    )

    if "Item" not in post_resp:
        return build_response(404, {"error": "Post not found"})

    # Delete the post
    posts_table.delete_item(
        Key={
            "post_id": post_id,
            "campaign_id": campaign_id
        }
    )

    return build_response(200, {
        "message": "Post deleted successfully",
        "post_id": post_id,
        "campaign_id": campaign_id
    })


def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }
