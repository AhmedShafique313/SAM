import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# DynamoDB
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("posts-table")


def lambda_handler(event, context):
    try:
        print("EVENT RECEIVED:", json.dumps(event))

        # CORS
        if event.get("httpMethod") == "OPTIONS":
            return _response(200, {"message": "CORS preflight"})

        body = event.get("body")
        if body and isinstance(body, str):
            body = json.loads(body)
        elif not body:
            body = event

        post_id = body.get("post_id")

        if not post_id:
            return _response(400, {"error": "post_id is required"})

        # 🔹 Step 1: Query by partition key (post_id)
        response = table.query(
            KeyConditionExpression=Key("post_id").eq(post_id),
            Limit=1
        )

        items = response.get("Items", [])
        if not items:
            return _response(
                404,
                {
                    "message": "Post not found",
                    "post_id": post_id
                }
            )

        post = items[0]
        campaign_id = post["campaign_id"]

        # 🔹 Step 2: Update ONLY status
        table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="SET #s = :draft",
            ExpressionAttributeNames={
                "#s": "status"
            },
            ExpressionAttributeValues={
                ":draft": "draft"
            }
        )

        return _response(
            200,
            {
                "message": "Post status updated to draft",
                "post_id": post_id
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


def _response(status_code, body):
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
