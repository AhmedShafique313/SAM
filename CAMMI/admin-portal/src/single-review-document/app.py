import json
import boto3
import os

dynamodb = boto3.resource("dynamodb")
review_table = dynamodb.Table( "review-documents-table" )

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",  # or your domain
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
}

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    if event.get("httpMethod") == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    response = review_table.scan()
    items = response.get("Items", [])

    while 'LastEvaluatedKey' in response:
        response = review_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    return build_response(200, {"documents": items})
