import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# Reuse client across invocations
dynamodb = boto3.resource('dynamodb')

TABLE_NAME = 'posts-table'
GSI_NAME = 'campaign_id-index'

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    """
    Fetch all posts for a given campaign_id using GSI,
    return them as a numbered post map.
    """

    # ✅ Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return _response(200, {"message": "CORS preflight"})

    body = event.get("body")
    if body:
        try:
            event = json.loads(body)
        except json.JSONDecodeError:
            return _response(400, {"error": "Invalid JSON in request body"})

    # 1. Extract input
    campaign_id = event.get('campaign_id')

    if not campaign_id:
        return _response(
            status_code=400,
            body={"error": "campaign_id is required"}
        )

    try:
        # 2. Query posts using GSI
        items = _query_by_campaign_id(campaign_id)

        # 3. Format as numbered post map
        posts_map = _format_numbered_posts(items)

        # 4. Success response
        return _response(
            status_code=200,
            body={
                "campaign_id": campaign_id,
                "posts": posts_map
            }
        )

    except ClientError as e:
        return _response(
            status_code=500,
            body={
                "error": "DynamoDB query failed",
                "details": e.response["Error"]["Message"]
            }
        )


def _query_by_campaign_id(campaign_id):
    """
    Query DynamoDB GSI with pagination support.
    """
    items = []
    exclusive_start_key = None

    while True:
        query_params = {
            "IndexName": GSI_NAME,
            "KeyConditionExpression": Key('campaign_id').eq(campaign_id)
        }

        if exclusive_start_key:
            query_params["ExclusiveStartKey"] = exclusive_start_key

        response = table.query(**query_params)
        items.extend(response.get('Items', []))

        exclusive_start_key = response.get('LastEvaluatedKey')
        if not exclusive_start_key:
            break

    return items


def _format_numbered_posts(items):
    """
    Convert the list of posts into a numbered post map:
    {
        "post-1": { ... },
        "post-2": { ... },
        ...
    }
    """
    numbered_posts = {}
    for idx, item in enumerate(items, start=1):
        numbered_posts[f"post-{idx}"] = item
    return numbered_posts


def _response(status_code, body):
    """Standardized HTTP response with CORS"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body, default=str)
    }
