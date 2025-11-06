import json
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal
 
# DynamoDB table
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('Wordpress-posts-table')  # updated table
 
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
        post_id = body.get('post_id')  # <-- new partition key
        publish_at = body.get('publish_at')  # <-- optional sort key
 
        if not post_id:
            return {
                'statusCode': 400,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
                },
                'body': json.dumps({'error': "'post_id' is required in the request body"})
            }
 
        # If publish_at is provided, query using both partition + sort key
        if publish_at:
            response = table.query(
                KeyConditionExpression=Key('post_id').eq(post_id) & Key('publish_at').eq(publish_at)
            )
        else:
            # Otherwise, just query by partition key (all posts with this post_id)
            response = table.query(
                KeyConditionExpression=Key('post_id').eq(post_id)
            )
 
        items = response.get('Items', [])
 
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
            },
            'body': json.dumps(items, default=decimal_default)
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
 
 