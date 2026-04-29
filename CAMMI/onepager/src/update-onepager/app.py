import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('onepager-table')

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'POST,PUT,OPTIONS',
    'Content-Type': 'application/json'
}

def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        body = json.loads(event.get('body') or '{}')

        project_id    = body.get('project_id')
        onepager_id   = body.get('onepager_id')
        title         = body.get('title')
        onepager_html = body.get('onepager_html')

        if not project_id or not onepager_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'project_id and onepager_id are required'})
            }

        if title is None and onepager_html is None:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'At least one of title or onepager_html must be provided'})
            }

        update_parts = []
        expr_attr_names  = {}
        expr_attr_values = {}

        if title is not None:
            update_parts.append('#title = :title')
            expr_attr_names['#title']  = 'title'
            expr_attr_values[':title'] = title

        if onepager_html is not None:
            update_parts.append('#onepager_html = :onepager_html')
            expr_attr_names['#onepager_html']  = 'onepager_html'
            expr_attr_values[':onepager_html'] = onepager_html

        update_parts.append('#updated_at = :updated_at')
        expr_attr_names['#updated_at']  = 'updated_at'
        expr_attr_values[':updated_at'] = datetime.now(timezone.utc).isoformat()

        update_expression = 'SET ' + ', '.join(update_parts)

        response = table.update_item(
            Key={
                'project_id':  project_id,
                'onepager_id': onepager_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expr_attr_names,
            ExpressionAttributeValues=expr_attr_values,
            ConditionExpression='attribute_exists(project_id) AND attribute_exists(onepager_id)',
            ReturnValues='ALL_NEW'
        )

        updated_item = response.get('Attributes', {})

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message':        'Onepager updated successfully',
                'project_id':     project_id,
                'onepager_id':    onepager_id,
                'updated_fields': [k for k in ['title', 'onepager_html'] if body.get(k) is not None],
                'updated_at':     updated_item.get('updated_at'),
                'item': {
                    'onepager_id':         updated_item.get('onepager_id'),
                    'project_id':          updated_item.get('project_id'),
                    'title':               updated_item.get('title'),
                    'slug':                updated_item.get('slug'),
                    'status':              updated_item.get('status'),
                    'onepager_brief':      updated_item.get('onepager_brief'),
                    'onepager_html':       updated_item.get('onepager_html'),
                    'onepager_output':     updated_item.get('onepager_output'),
                    'generation_metadata': updated_item.get('generation_metadata'),
                    'created_at':          updated_item.get('created_at'),
                    'updated_at':          updated_item.get('updated_at'),
                }
            }, default=str)
        }

    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return {
            'statusCode': 404,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Record not found for the given project_id and onepager_id'})
        }
    except json.JSONDecodeError:
        return {
            'statusCode': 400,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Invalid JSON in request body'})
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Internal server error', 'details': str(e)})
        }
