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
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for updating document name in DynamoDB and S3
    
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
        new_document_name = body['document_name']
        
        # Get current document from DynamoDB
        current_document = get_document_from_dynamodb(user_id, document_type_uuid)
        
        if not current_document:
            return create_response(404, {'error': 'Document not found'})
        
        old_document_url = current_document.get('document_url')
        old_document_name = current_document.get('document_name')
        
        if not old_document_url:
            return create_response(400, {'error': 'Document URL not found in the record'})
        
        # Rename file in S3
        new_document_url = rename_file_in_s3(old_document_url, old_document_name, new_document_name)
        
        # Update DynamoDB record
        updated_item = update_document_in_dynamodb(
            user_id, 
            document_type_uuid, 
            new_document_name, 
            new_document_url
        )
        
        # Prepare success response
        response_data = {
            'message': 'Document name updated successfully',
            'updated_item': {
                'user_id': user_id,
                'document_type_uuid': document_type_uuid,
                'old_document_name': old_document_name,
                'new_document_name': new_document_name,
                'old_document_url': old_document_url,
                'new_document_url': new_document_url
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
    required_params = ['user_id', 'document_type_uuid', 'document_name']
    missing_params = [param for param in required_params if not body.get(param)]
    
    if missing_params:
        return {
            'error': 'Missing required parameters',
            'missing': missing_params,
            'required': required_params
        }
    
    # Validate that document_name is not empty
    if not body['document_name'].strip():
        return {
            'error': 'Invalid parameter',
            'details': 'document_name cannot be empty'
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


def update_document_in_dynamodb(
    user_id: str, 
    document_type_uuid: str, 
    new_document_name: str, 
    new_document_url: str
) -> Dict[str, Any]:
    """
    Update document name and URL in DynamoDB
    
    Args:
        user_id: User ID (partition key)
        document_type_uuid: Document type UUID (sort key)
        new_document_name: New document name
        new_document_url: New document URL
    
    Returns:
        Updated item from DynamoDB
    
    Raises:
        ClientError: If DynamoDB update fails
    """
    response = table.update_item(
        Key={
            'user_id': user_id,
            'document_type_uuid': document_type_uuid
        },
        UpdateExpression='SET document_name = :new_name, document_url = :new_url',
        ExpressionAttributeValues={
            ':new_name': new_document_name,
            ':new_url': new_document_url
        },
        ReturnValues='ALL_NEW'
    )
    
    return response.get('Attributes', {})


def rename_file_in_s3(old_url: str, old_name: str, new_name: str) -> str:
    """
    Rename a file in S3 by copying and deleting
    
    Args:
        old_url: Current S3 URL
        old_name: Current document name
        new_name: New document name
    
    Returns:
        New S3 URL after renaming
    
    Raises:
        ValueError: If URL format is invalid
        ClientError: If S3 operation fails
    """
    bucket_name, old_key = parse_s3_url(old_url)
    new_key = generate_new_s3_key(old_key, old_name, new_name)
    
    # Copy object to new key
    copy_s3_object(bucket_name, old_key, new_key)
    
    # Delete old object
    delete_s3_object(bucket_name, old_key)
    
    # Generate new URL in the same format as original
    new_url = construct_s3_url(old_url, bucket_name, new_key)
    
    return new_url


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


def generate_new_s3_key(old_key: str, old_name: str, new_name: str) -> str:
    """
    Generate new S3 key with updated filename
    
    Args:
        old_key: Current S3 key
        old_name: Current document name
        new_name: New document name
    
    Returns:
        New S3 key with updated filename
    """
    # Extract file extension
    file_extension = ''
    if '.' in old_key:
        file_extension = '.' + old_key.rsplit('.', 1)[1]
    
    # Ensure new name has the same extension
    if file_extension and not new_name.endswith(file_extension):
        new_filename = new_name + file_extension
    else:
        new_filename = new_name
    
    # Replace filename in the key path
    key_parts = old_key.rsplit('/', 1)
    if len(key_parts) > 1:
        new_key = f"{key_parts[0]}/{new_filename}"
    else:
        new_key = new_filename
    
    return new_key


def copy_s3_object(bucket_name: str, source_key: str, destination_key: str) -> None:
    """
    Copy S3 object from source to destination
    
    Args:
        bucket_name: S3 bucket name
        source_key: Source object key
        destination_key: Destination object key
    
    Raises:
        ClientError: If S3 copy operation fails
    """
    copy_source = {
        'Bucket': bucket_name,
        'Key': source_key
    }
    
    s3_client.copy_object(
        CopySource=copy_source,
        Bucket=bucket_name,
        Key=destination_key
    )


def delete_s3_object(bucket_name: str, key: str) -> None:
    """
    Delete S3 object
    
    Args:
        bucket_name: S3 bucket name
        key: Object key to delete
    
    Raises:
        ClientError: If S3 delete operation fails
    """
    s3_client.delete_object(
        Bucket=bucket_name,
        Key=key
    )


def construct_s3_url(original_url: str, bucket_name: str, new_key: str) -> str:
    """
    Construct S3 URL in the same format as original
    
    Args:
        original_url: Original S3 URL for format reference
        bucket_name: S3 bucket name
        new_key: New object key
    
    Returns:
        New S3 URL in the same format as original
    """
    parsed_url = urlparse(original_url)
    
    if parsed_url.scheme == 's3':
        return f"s3://{bucket_name}/{new_key}"
    
    # Reconstruct HTTPS URL
    return f"{parsed_url.scheme}://{parsed_url.hostname}/{new_key}"