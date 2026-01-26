import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# Reuse client across invocations (best practice)
dynamodb = boto3.resource('dynamodb')

TABLE_NAME = 'user-campaigns'
GSI_NAME = 'project_id-index'

table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    """
    Fetch all campaigns for a given project_id
    and return them as a clean list.
    """

    # 1. Extract inputs
    session_id = event.get('session_id')
    project_id = event.get('project_id')

    if not project_id:
        return _response(
            status_code=400,
            body={"error": "project_id is required"}
        )

    try:
        # 2. Query using GSI (pagination-safe)
        items = _query_by_project_id(project_id)

        # 3. Format campaigns (no grouping needed)
        campaigns = _format_campaigns(items)

        # 4. Success response
        return _response(
            status_code=200,
            body={
                "session_id": session_id,
                "project_id": project_id,
                "campaigns": campaigns
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


def _query_by_project_id(project_id):
    """Query DynamoDB GSI with pagination support"""
    items = []
    exclusive_start_key = None

    while True:
        query_params = {
            "IndexName": GSI_NAME,
            "KeyConditionExpression": Key('project_id').eq(project_id)
        }

        if exclusive_start_key:
            query_params["ExclusiveStartKey"] = exclusive_start_key

        response = table.query(**query_params)

        items.extend(response.get('Items', []))

        exclusive_start_key = response.get('LastEvaluatedKey')
        if not exclusive_start_key:
            break

    return items


def _format_campaigns(items):
    """
    Return campaigns as a numbered dictionary:
    campaign-1, campaign-2, ...
    """
    formatted = {}

    for index, item in enumerate(items, start=1):
        formatted[f"campaign-{index}"] = item

    return formatted


def _response(status_code, body):
    """Standardized HTTP response"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(body, default=str)
    }
