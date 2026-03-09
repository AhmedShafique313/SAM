import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('linkedin-user-table')

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST"
}

def lambda_handler(event, context):

    # Handle preflight OPTIONS request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight success"})
        }

    try:
        # Parse body from frontend request
        body = json.loads(event['body'])
        sub = body.get('sub')

        if not sub:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "message": "sub is required"
                })
            }

        # Delete item from DynamoDB
        table.delete_item(
            Key={
                'sub': sub
            }
        )

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Logout successful"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Error during logout",
                "error": str(e)
            })
        }