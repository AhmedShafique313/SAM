import json
import boto3
from collections import defaultdict

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('payment-history-table')

    # Scan the entire table
    response = table.scan()
    items = response.get('Items', [])

    # Handle pagination (if there are more than 1MB of items)
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    # Group items by email and sum total spent
    grouped_data = defaultdict(lambda: {"records": [], "total_spent": 0, "name": None})
    for item in items:
        email = item.get('email', 'unknown')
        grouped_data[email]["records"].append(item)
        grouped_data[email]["total_spent"] += float(item.get('amount_total', 0))

        # Capture name if available (only once)
        if not grouped_data[email]["name"]:
            name = item.get('name')
            if name:
                grouped_data[email]["name"] = name

    # Convert defaultdict to normal dict for JSON serialization
    grouped_data = dict(grouped_data)

    # Success response
    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps({
            "success": True,
            "data": grouped_data
        }, default=str)
    }
