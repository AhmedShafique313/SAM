import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
 
# DynamoDB table
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('linkedin-posts-table')
 
# Helper function to convert Decimal to float/int
def decimal_default(obj):
    if isinstance(obj, Decimal):
        # Convert to int if no fractional part, else float
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    raise TypeError
 
def lambda_handler(event, context):
    try:
        body = json.loads(event.get('body', '{}'))
        sub_value = body.get('sub')
        if not sub_value:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({'error': "'sub' is required in the request body"})
            }
 
        response = table.query(
            KeyConditionExpression=Key('sub').eq(sub_value)
        )
        items = response.get('Items', [])
 
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(items, default=decimal_default)  # <--- fix here
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps({'error': str(e)})
        }
 
 