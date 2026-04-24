import json
import boto3

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

        response = table.delete_item(
            Key={
                'project_id': project_id,
                'blog_id': blog_id
            },
            ConditionExpression='attribute_exists(project_id) AND attribute_exists(blog_id)',
            ReturnValues='ALL_OLD'
        )

        deleted_item = response.get('Attributes', {})

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'message': 'Blog deleted successfully',
                'deleted_blog': deleted_item
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
