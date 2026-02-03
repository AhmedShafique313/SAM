import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("posts-table")

GSI_NAME = "campaign_id-index"


def lambda_handler(event, context):
    """
    PUT /posts/draft
    """

    try:
        print("EVENT RECEIVED:", json.dumps(event))

        # Handle CORS preflight (OPTIONS)
        if event.get("httpMethod") == "OPTIONS":
            return _response(200, {"message": "CORS preflight"})

        # Extract body (API Gateway / Lambda console safe)
        body = event.get("body")

        if body:
            if isinstance(body, str):
                body = json.loads(body)
        else:
            # Lambda console fallback
            body = event

        campaign_id = body.get("campaign_id")

        if not campaign_id:
            return _response(400, {"error": "campaign_id is required"})

        # Query posts
        posts = _query_posts_by_campaign(campaign_id)

        if not posts:
            return _response(
                404,
                {
                    "message": "No posts found for campaign",
                    "campaign_id": campaign_id
                }
            )

        # Update only posts with status "Generated"
        updated_posts = 0
        for post in posts:
            if post.get("status") == "Generated":
                table.update_item(
                    Key={
                        "post_id": post["post_id"],
                        "campaign_id": post["campaign_id"]
                    },
                    UpdateExpression="SET #status = :draft",
                    ExpressionAttributeNames={
                        "#status": "status"
                    },
                    ExpressionAttributeValues={
                        ":draft": "draft"
                    }
                )
                updated_posts += 1

        return _response(
            200,
            {
                "message": "Posts updated to draft",
                "campaign_id": campaign_id,
                "updated_posts": updated_posts
            }
        )

    except json.JSONDecodeError:
        return _response(400, {"error": "Invalid JSON in request body"})

    except ClientError as e:
        return _response(
            500,
            {
                "error": "DynamoDB operation failed",
                "details": e.response["Error"]["Message"]
            }
        )

    except Exception as e:
        return _response(
            500,
            {
                "error": "Internal server error",
                "details": str(e)
            }
        )


def _query_posts_by_campaign(campaign_id):
    """Query DynamoDB GSI with pagination"""
    items = []
    last_evaluated_key = None

    while True:
        params = {
            "IndexName": GSI_NAME,
            "KeyConditionExpression": Key("campaign_id").eq(campaign_id)
        }

        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        response = table.query(**params)
        items.extend(response.get("Items", []))

        last_evaluated_key = response.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break

    return items


def _response(status_code, body):
    """Standard API Gateway response with CORS"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,PUT,GET,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
