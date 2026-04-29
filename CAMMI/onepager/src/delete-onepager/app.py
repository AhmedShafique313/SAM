import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('onepager-table')

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'DELETE,POST,OPTIONS',
    'Content-Type': 'application/json'
}

def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        body = json.loads(event.get('body') or '{}')

        project_id  = body.get('project_id')
        onepager_id = body.get('onepager_id')

        if not project_id or not onepager_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'project_id and onepager_id are required'})
            }

        response = table.delete_item(
            Key={
                'project_id':  project_id,
                'onepager_id': onepager_id
            },
            ConditionExpression='attribute_exists(project_id) AND attribute_exists(onepager_id)',
            ReturnValues='ALL_OLD'
        )

        deleted_item = response.get('Attributes', {})

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message':     'Onepager deleted successfully',
                'project_id':  project_id,
                'onepager_id': onepager_id,
                'deleted_item': {
                    'onepager_id':         deleted_item.get('onepager_id'),
                    'project_id':          deleted_item.get('project_id'),
                    'title':               deleted_item.get('title'),
                    'slug':                deleted_item.get('slug'),
                    'status':              deleted_item.get('status'),
                    'onepager_brief':      deleted_item.get('onepager_brief'),
                    'onepager_html':       deleted_item.get('onepager_html'),
                    'onepager_output':     deleted_item.get('onepager_output'),
                    'generation_metadata': deleted_item.get('generation_metadata'),
                    'created_at':          deleted_item.get('created_at'),
                    'updated_at':          deleted_item.get('updated_at'),
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
