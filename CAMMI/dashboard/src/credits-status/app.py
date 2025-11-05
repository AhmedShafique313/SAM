import json
import boto3
from boto3.dynamodb.conditions import Attr
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
USERS_TABLE = 'users-table'

def lambda_handler(event, context):
    body = json.loads(event.get('body', '{}'))
    session_id = body.get('session_id')

    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Content-Type': 'application/json'
    }

    # Handle missing session_id
    if not session_id:
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Missing session_id'})
        }

    # Query DynamoDB for the session_id
    table = dynamodb.Table(USERS_TABLE)
    response = table.scan(
        FilterExpression=Attr('session_id').eq(session_id)
    )

    items = response.get('Items', [])

    # Handle not found
    if not items:
        return {
            'statusCode': 404,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Session not found'})
        }

    user_item = items[0]
    total_credits = user_item.get('total_credits', 0)

    # Convert Decimal → int or float
    if isinstance(total_credits, Decimal):
        total_credits = int(total_credits) if total_credits % 1 == 0 else float(total_credits)

    # Successful response
    return {
        'statusCode': 200,
        'headers': cors_headers,
        'body': json.dumps({'total_credits': total_credits})
    }
