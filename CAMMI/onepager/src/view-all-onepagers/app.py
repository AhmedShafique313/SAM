import json
import re
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('onepager-table')
s3_client = boto3.client('s3')

CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
    'Access-Control-Allow-Methods': 'POST,OPTIONS',
    'Content-Type': 'application/json'
}

S3_BUCKET_NAME = 'cammi-devprod'
PRESIGNED_URL_EXPIRY = 3600  # 1 hour in seconds


def replace_s3_urls_with_presigned(html_content):
    """
    Find all S3 URLs in img src attributes and replace them with pre-signed URLs.
    """
    if not html_content:
        return html_content

    s3_url_pattern = re.compile(
        r'(src=["\'])https://([^.]+)\.s3(?:\.[^.]+)?\.amazonaws\.com/([^"\'>\s]+)(["\'])',
        re.IGNORECASE
    )

    def replace_match(match):
        prefix = match.group(1)
        bucket_name = match.group(2)
        s3_key = match.group(3)
        suffix = match.group(4)

        try:
            presigned_url = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': bucket_name, 'Key': s3_key},
                ExpiresIn=PRESIGNED_URL_EXPIRY
            )
            return f'{prefix}{presigned_url}{suffix}'
        except Exception as e:
            print(f"Failed to generate pre-signed URL for key {s3_key}: {e}")
            return match.group(0)

    return s3_url_pattern.sub(replace_match, html_content)


def lambda_handler(event, context):
    if event.get('httpMethod') == 'OPTIONS':
        return {'statusCode': 200, 'headers': CORS_HEADERS, 'body': ''}

    try:
        body = json.loads(event.get('body') or '{}')
        project_id = body.get('project_id')

        if not project_id:
            return {
                'statusCode': 400,
                'headers': CORS_HEADERS,
                'body': json.dumps({'error': 'project_id is required'})
            }

        response = table.query(
            KeyConditionExpression=Key('project_id').eq(project_id)
        )

        items = response.get('Items', [])

        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('project_id').eq(project_id),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response.get('Items', []))

        result = []
        for item in items:
            raw_html = item.get('onepager_html')
            signed_html = replace_s3_urls_with_presigned(raw_html)

            result.append({
                'onepager_id':         item.get('onepager_id'),
                'project_id':          item.get('project_id'),
                'title':               item.get('title'),
                'slug':                item.get('slug'),
                'status':              item.get('status'),
                'onepager_brief':      item.get('onepager_brief'),
                'onepager_html':       signed_html,
                'onepager_output':     item.get('onepager_output'),
                'generation_metadata': item.get('generation_metadata'),
                'created_at':          item.get('created_at'),
                'updated_at':          item.get('updated_at'),
            })

        return {
            'statusCode': 200,
            'headers': CORS_HEADERS,
            'body': json.dumps({
                'project_id': project_id,
                'count': len(result),
                'onepagers': result
            }, default=str)
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
