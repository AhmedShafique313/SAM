import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Organizations-table")  # Change if your actual table name differs


def lambda_handler(event, context):
    try:
        # Scan the Organizations table
        response = table.scan()
        items = response.get("Items", [])
        total_count = len(items)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",   # Allow all origins
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "total_organizations": total_count,
                "organizations": items
            })
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": str(e)})
        }
