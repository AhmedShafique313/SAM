import json
import boto3
import os
from typing import Dict, Any, Optional
from botocore.exceptions import ClientError

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

# Table names from environment variables
DOCUMENT_TABLE_NAME = os.environ.get('DOCUMENT_TABLE_NAME', 'documents-history-table')
USERS_TABLE_NAME = os.environ.get('USERS_TABLE_NAME', 'users-table')
PROJECTS_TABLE_NAME = os.environ.get('PROJECTS_TABLE_NAME', 'projects-table')
ORGANIZATIONS_TABLE_NAME = os.environ.get('ORGANIZATIONS_TABLE_NAME', 'organizations-table')

# Initialize tables
document_table = dynamodb.Table(DOCUMENT_TABLE_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)
projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)
organizations_table = dynamodb.Table(ORGANIZATIONS_TABLE_NAME)

# CORS headers
CORS_HEADERS = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Requested-With'
}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for retrieving document information with related data
    
    Args:
        event: Lambda event object containing request body
        context: Lambda context object
    
    Returns:
        API Gateway response with simplified document data
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
        
        # Get document from DocumentHistory
        document = get_document_from_dynamodb(user_id, document_type_uuid)
        
        if not document:
            return create_response(404, {'error': 'Document not found'})
        
        # Get user information
        user = get_user_from_dynamodb(user_id)
        
        # Get project information
        project = None
        organization = None
        
        project_id = document.get('project_id')
        if project_id:
            project = get_project_from_dynamodb(project_id)
            
            # Get organization information
            if project:
                organization_id = project.get('organization_id')
                if organization_id:
                    organization = get_organization_from_dynamodb(organization_id)
        
        # Build simplified response data
        response_data = build_response_data(document, user, project, organization)
        
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
        'body': json.dumps(body, default=str)
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


def validate_request_parameters(body: Dict[str, Any]) -> Optional[Dict[str, Any]]:
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


def get_document_from_dynamodb(user_id: str, document_type_uuid: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve document from DocumentHistory table
    
    Args:
        user_id: User ID (partition key)
        document_type_uuid: Document type UUID (sort key)
    
    Returns:
        Document item if found, None otherwise
    
    Raises:
        ClientError: If DynamoDB operation fails
    """
    response = document_table.get_item(
        Key={
            'user_id': user_id,
            'document_type_uuid': document_type_uuid
        }
    )
    
    return response.get('Item')


def get_user_from_dynamodb(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve user from Users table using GSI
    
    Note: Users table partition key is 'email', but we receive 'id' from DocumentHistory.
    This function uses a GSI named 'id-index' to efficiently query by id.
    
    REQUIREMENT: GSI 'id-index' must exist on Users table with 'id' as partition key.
    
    Args:
        user_id: User ID from DocumentHistory
    
    Returns:
        User item if found, None otherwise
    
    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        # Method 1: If user_id is an email, use direct lookup (fastest)
        if '@' in user_id:
            print(f"user_id is email, using direct lookup: {user_id}")
            response = users_table.get_item(
                Key={
                    'email': user_id
                }
            )
            item = response.get('Item')
            if item:
                print(f"User found by email: {user_id}")
                return item
        
        # Method 2: Query using GSI 'id-index' (required for non-email user_ids)
        print(f"Querying Users table using GSI id-index for user_id: {user_id}")
        response = users_table.query(
            IndexName='id-index',
            KeyConditionExpression='id = :user_id',
            ExpressionAttributeValues={
                ':user_id': user_id
            }
        )
        
        items = response.get('Items', [])
        if items:
            user = items[0]
            print(f"User found using GSI: {user_id}")
            print(f"User data - name: {user.get('name')}, firstName: {user.get('firstName')}, lastName: {user.get('lastName')}")
            return user
        
        print(f"User not found with id: {user_id}")
        return None
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        if error_code == 'ResourceNotFoundException':
            print(f"ERROR: GSI 'id-index' does not exist on Users table. Please create it.")
            print(f"Error details: {error_message}")
        else:
            print(f"Error retrieving user: {error_code} - {error_message}")
        
        return None


def get_project_from_dynamodb(project_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve project from projects table
    
    Args:
        project_id: Project ID (partition key)
    
    Returns:
        Project item if found, None otherwise
    
    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        response = projects_table.get_item(
            Key={
                'id': project_id
            }
        )
        return response.get('Item')
    except ClientError as e:
        # If project not found, log but don't fail the entire request
        print(f"Project not found: {project_id}, Error: {str(e)}")
        return None


def get_organization_from_dynamodb(organization_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve organization from Organizations table
    
    Args:
        organization_id: Organization ID (partition key)
    
    Returns:
        Organization item if found, None otherwise
    
    Raises:
        ClientError: If DynamoDB operation fails
    """
    try:
        response = organizations_table.get_item(
            Key={
                'id': organization_id
            }
        )
        return response.get('Item')
    except ClientError as e:
        # If organization not found, log but don't fail the entire request
        print(f"Organization not found: {organization_id}, Error: {str(e)}")
        return None


def build_response_data(
    document: Dict[str, Any],
    user: Optional[Dict[str, Any]],
    project: Optional[Dict[str, Any]],
    organization: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Build the simplified response data structure with only required fields
    
    Args:
        document: Document data from DocumentHistory
        user: User data from Users table
        project: Project data from projects table
        organization: Organization data from Organizations table
    
    Returns:
        Simplified response dictionary with only 6 fields
    """
    # Get user name (try name field first, then combine firstName and lastName)
    user_name = None
    if user:
        print(f"Building response with user data. User keys: {list(user.keys())}")
        user_name = user.get('name')
        print(f"User 'name' field: {user_name}")
        
        if not user_name:
            first_name = user.get('firstName', '')
            last_name = user.get('lastName', '')
            print(f"firstName: {first_name}, lastName: {last_name}")
            user_name = f"{first_name} {last_name}".strip() or None
            print(f"Combined user_name: {user_name}")
    else:
        print("No user data found")
    
    response = {
        'organization_name': organization.get('organization_name') if organization else None,
        'project_name': project.get('project_name') if project else None,
        'document_name': document.get('document_name'),
        'document_type': document.get('document_type'),
        'created_at': document.get('created_at'),
        'user_name': user_name
    }
    
    return response