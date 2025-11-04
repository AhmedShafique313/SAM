import json
import boto3
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')

# DynamoDB table names
TABLES = {
    "Users": "users-table",
    "Organizations": "organizations-table",
    "Projects": "projects-table",
    "ReviewDocument": "review-documents-table"
}

def humanize_time_diff(created_at_str):
    """Convert an ISO date or timestamp to a human-readable difference."""
    try:
        created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    except Exception:
        # Try timestamp fallback
        try:
            created_at = datetime.fromtimestamp(float(created_at_str))
        except Exception:
            return "unknown time"

    # ✅ Ensure both are timezone-aware (UTC)
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    diff = now - created_at

    seconds = diff.total_seconds()
    minutes = seconds / 60
    hours = minutes / 60
    days = hours / 24
    years = days / 365

    if seconds < 60:
        return f"{int(seconds)} sec ago"
    elif minutes < 60:
        return f"{int(minutes)} min ago"
    elif hours < 24:
        return f"{int(hours)} hour{'s' if int(hours) != 1 else ''} ago"
    elif days < 30:
        return f"{int(days)} day{'s' if int(days) != 1 else ''} ago"
    elif days < 365:
        months = int(days // 30)
        return f"{months} month{'s' if months != 1 else ''} ago"
    else:
        return f"{int(years)} year{'s' if int(years) != 1 else ''} ago"


def get_latest_record(table_name):
    """Scan the DynamoDB table and return the latest record by createdAt."""
    table = dynamodb.Table(table_name)
    response = table.scan()
    items = response.get('Items', [])

    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response.get('Items', []))

    if not items:
        return None

    # Sort by createdAt descending
    items.sort(key=lambda x: x.get('createdAt', ''), reverse=True)
    latest = items[0]

    created_at = latest.get('createdAt')
    relative_time = humanize_time_diff(created_at)

    return {
        "table": table_name,
        "createdAt": created_at,
        "relative_time": relative_time
    }


def lambda_handler(event, context):
    results = {}
    for key, table_name in TABLES.items():
        latest_record = get_latest_record(table_name)
        results[key] = latest_record if latest_record else {"message": "No records found"}

    response = {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",  # Allow all origins (adjust if needed)
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
            "Content-Type": "application/json"
        },
        "body": json.dumps({
            "success": True,
            "data": results
        })
    }

    return response
