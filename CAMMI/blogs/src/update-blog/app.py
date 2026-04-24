import json
import boto3
from boto3.dynamodb.conditions import Key
from datetime import datetime, timezone

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('blogs-table')

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'POST,OPTIONS'
}

def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({'message': 'CORS preflight'})
        }

    try:
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)

        project_id = body.get('project_id')
        blog_id    = body.get('blog_id')
        title      = body.get('title')
        blog_html  = body.get('blog_html')

        if not project_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'project_id is required'})
            }
        if not blog_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'blog_id is required'})
            }
        if not title and not blog_html:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'At least one of title or blog_html is required'})
            }

        update_parts = []
        expression_values = {}
        expression_names = {}

        if title:
            update_parts.append('#title = :title')
            expression_values[':title'] = title
            expression_names['#title'] = 'title'

        if blog_html:
            update_parts.append('blog_html = :blog_html')
            expression_values[':blog_html'] = blog_html

        update_parts.append('updated_at = :updated_at')
        expression_values[':updated_at'] = datetime.now(timezone.utc).isoformat()

        update_expression = 'SET ' + ', '.join(update_parts)

        response = table.update_item(
            Key={
                'project_id': project_id,
                'blog_id': blog_id
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names if expression_names else None,
            ConditionExpression='attribute_exists(project_id) AND attribute_exists(blog_id)',
            ReturnValues='ALL_NEW'
        )

        updated_item = response.get('Attributes', {})

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Blog updated successfully',
                'blog': updated_item
            }, default=str)
        }

    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        return {
            'statusCode': 404,
            'headers': CORS_HEADERS,
            'body': json.dumps({'error': 'Blog not found with the given project_id and blog_id'})
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
            'body': json.dumps({'error': str(e)})
        }
