import boto3
import json
import os

# Initialize DynamoDB client
dynamodb = boto3.client("dynamodb")

# Get table names from environment variables (recommended for flexibility)
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
ORGANIZATIONS_TABLE = os.environ.get("ORGANIZATIONS_TABLE", "organizations-table")
PROJECTS_TABLE = os.environ.get("PROJECTS_TABLE", "projects-table")
REVIEW_DOCUMENTS_TABLE = os.environ.get("REVIEW_DOCUMENTS_TABLE", "review-document-table")


def lambda_handler(event, context):
    try:
        # Get table counts
        users_count = get_table_count(USERS_TABLE)
        organizations_count = get_table_count(ORGANIZATIONS_TABLE)
        projects_count = get_table_count(PROJECTS_TABLE)
        review_documents_count = get_table_count(REVIEW_DOCUMENTS_TABLE)

        response_body = {
            "users_count": users_count,
            "organizations_count": organizations_count,
            "projects_count": projects_count,
            "reviewDocuments_count": review_documents_count
        }

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",   # Allow all origins
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps(response_body)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": str(e)})
        }


def get_table_count(table_name):
    """
    Fetch the count of items in a DynamoDB table using scan with Select='COUNT'
    """
    response = dynamodb.scan(
        TableName=table_name,
        Select="COUNT"
    )
    return response["Count"]
