import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
PROJECTS_TABLE = "projects-table"
projects_table = dynamodb.Table(PROJECTS_TABLE)

def lambda_handler(event, context):
    try:
        # Get organization_id from headers
        headers = event.get("headers", {})
        organization_id = headers.get("organization_id")

        if not organization_id:
            return response(400, {"error": "organization_id header is required"})

        # Query projects table for this organization_id
        projects_resp = projects_table.scan(
            FilterExpression=Attr("organization_id").eq(organization_id)
        )

        projects = projects_resp.get("Items", [])

        return response(200, {
            "message": "Projects fetched successfully",
            "projects": projects
        })

    except Exception as e:
        return response(500, {"error": str(e)})


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,organization_id"
        },
        "body": json.dumps(body)
    }
