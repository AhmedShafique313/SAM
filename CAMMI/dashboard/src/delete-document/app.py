import json
import boto3
import os
from typing import Dict, Any, Tuple
from urllib.parse import urlparse
from botocore.exceptions import ClientError

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# Constants
TABLE_NAME = os.environ.get('DYNAMODB_TABLE_NAME', 'documents-history-table')
table = dynamodb.Table(TABLE_NAME)

# CORS headers
CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for deleting document from DynamoDB and S3
    
    Args:
        event: Lambda event object containing request body
        context: Lambda context object
    
    Returns:
        API Gateway response with status code and body
    """
    
    # Handle OPTIONS request for CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        return create_response(200, {'message': 'OK'})
    
    try:
        # Parse and validate request
        body = parse_request_body(event)
        validation_error = validate_request_parameters(body)
        
        if validation_error:
            return create_response(400, validation_error)
        
        user_id = body['user_id']
        document_type_uuid = body['document_type_uuid']
        
        # Get current document from DynamoDB
        current_document = get_document_from_dynamodb(user_id, document_type_uuid)
        
        if not current_document:
            return create_response(404, {'error': 'Document not found'})
        
        document_url = current_document.get('document_url')
        document_name = current_document.get('document_name')
        
        # Delete file from S3 if document_url exists
        if document_url:
            delete_file_from_s3(document_url)
        
        # Delete entry from DynamoDB
        delete_document_from_dynamodb(user_id, document_type_uuid)
        
        # Prepare success response
        response_data = {
            'message': 'Document deleted successfully',
            'deleted_item': {
                'user_id': user_id,
                'document_type_uuid': document_type_uuid,
                'document_name': document_name,
                'document_url': document_url
            }
        }
        
        return create_response(200, response_data)
        
    except ClientError as e:
        error_message = f"AWS service error: {e.response['Error']['Message']}"
        print(f"ClientError: {error_message}")
        return create_response(500, {
            'error': 'AWS service error',
            'details': e.response['Error']['Message']
        })
    
    except ValueError as e:
        print(f"ValueError: {str(e)}")
        return create_response(400, {
            'error': 'Invalid request',
            'details': str(e)
        })
    
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return create_response(500, {
            'error': 'Internal server error',
            'details': str(e)
        })


def create_response(status_code: int, body: Dict[str, Any]) -> Dict[str, Any]:
    """
    Create standardized API Gateway response with CORS headers
    
    Args:
        status_code: HTTP status code
        body: Response body dictionary
    
    Returns:
        Formatted API Gateway response
    """
    return {
        'statusCode': status_code,
        'headers': CORS_HEADERS,
        'body': json.dumps(body)
    }


def parse_request_body(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and extract body from Lambda event
    
    Args:
        event: Lambda event object
    
    Returns:
        Parsed body dictionary
    
    Raises:
        ValueError: If body cannot be parsed
    """
    try:
        body_str = event.get('body', '{}')
        
        # Handle case where body is already a dict
        if isinstance(body_str, dict):
            return body_str
        
        return json.loads(body_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in request body: {str(e)}")


def validate_request_parameters(body: Dict[str, Any]) -> Dict[str, Any] or None:
    """
    Validate required parameters in request body
    
    Args:
        body: Request body dictionary
    
    Returns:
        Error dictionary if validation fails, None if successful
    """
    required_params = ['user_id', 'document_type_uuid']
    missing_params = [param for param in required_params if not body.get(param)]
    
    if missing_params:
        return {
            'error': 'Missing required parameters',
            'missing': missing_params,
            'required': required_params
        }
    
    return None


def get_document_from_dynamodb(user_id: str, document_type_uuid: str) -> Dict[str, Any] or None:
    """
    Retrieve document from DynamoDB
    
    Args:
        user_id: User ID (partition key)
        document_type_uuid: Document type UUID (sort key)
    
    Returns:
        Document item if found, None otherwise
    
    Raises:
        ClientError: If DynamoDB operation fails
    """
    response = table.get_item(
        Key={
            'user_id': user_id,
            'document_type_uuid': document_type_uuid
        }
    )
    
    return response.get('Item')


def delete_document_from_dynamodb(user_id: str, document_type_uuid: str) -> None:
    """
    Delete document from DynamoDB
    
    Args:
        user_id: User ID (partition key)
        document_type_uuid: Document type UUID (sort key)
    
    Raises:
        ClientError: If DynamoDB delete operation fails
    """
    table.delete_item(
        Key={
            'user_id': user_id,
            'document_type_uuid': document_type_uuid
        }
    )


def delete_file_from_s3(document_url: str) -> None:
    """
    Delete file from S3
    
    Args:
        document_url: S3 URL of the document to delete
    
    Raises:
        ValueError: If URL format is invalid
        ClientError: If S3 delete operation fails
    """
    bucket_name, key = parse_s3_url(document_url)
    
    # Delete the object from S3
    s3_client.delete_object(
        Bucket=bucket_name,
        Key=key
    )
    
    print(f"Deleted S3 object: s3://{bucket_name}/{key}")


def parse_s3_url(url: str) -> Tuple[str, str]:
    """
    Parse S3 URL to extract bucket name and key
    
    Args:
        url: S3 URL (s3:// or https://)
    
    Returns:
        Tuple of (bucket_name, key)
    
    Raises:
        ValueError: If URL format is not supported
    """
    parsed_url = urlparse(url)
    
    # Handle s3:// URLs
    if parsed_url.scheme == 's3':
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')
        return bucket_name, key
    
    # Handle https:// URLs
    if parsed_url.scheme in ['http', 'https']:
        hostname_parts = parsed_url.hostname.split('.')
        
        # Format: s3.region.amazonaws.com/bucket/key
        if hostname_parts[0] == 's3':
            path_parts = parsed_url.path.lstrip('/').split('/', 1)
            bucket_name = path_parts[0]
            key = path_parts[1] if len(path_parts) > 1 else ''
            return bucket_name, key
        
        # Format: bucket.s3.region.amazonaws.com/key
        bucket_name = hostname_parts[0]
        key = parsed_url.path.lstrip('/')
        return bucket_name, key
    
    raise ValueError(f"Unsupported URL scheme: {parsed_url.scheme}")