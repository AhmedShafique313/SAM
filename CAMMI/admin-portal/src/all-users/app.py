import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
USERS_TABLE = "users-table"
users_table = dynamodb.Table(USERS_TABLE)

# Helper function to handle Decimal values
def decimal_default(obj):
    if isinstance(obj, Decimal):
        # Convert to int if no fractional part, else to float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    try:
        # Scan all users
        response = users_table.scan()
        users = response.get("Items", [])

        normalized_users = []
        for user in users:
            # Build full name
            if user.get("name"):  # some users have full name in 'name'
                full_name = user["name"]
            else:
                first = user.get("firstName", "")
                last = user.get("lastName", "")
                full_name = f"{first} {last}".strip()

            # Remove firstName, lastName, name and add fullName
            normalized_user = {k: v for k, v in user.items()
                               if k not in ["firstName", "lastName", "name"]}
            normalized_user["fullName"] = full_name

            normalized_users.append(normalized_user)

        # Count of users
        user_count = len(normalized_users)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",  # Allow all origins
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "count": user_count,
                "users": normalized_users
            }, default=decimal_default)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": str(e)})
        }
