import json
import boto3
import os
from botocore.exceptions import ClientError

# -------------------------------------------------------
#  HARDCODED RESOURCES
# -------------------------------------------------------
USERS_TABLE_NAME = "users-table"
bucket_name = "cammi-devprod"
WEBSOCKET_ENDPOINT = os.environ["WEBSOCKET_ENDPOINT1"]
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
users_table = dynamodb.Table(USERS_TABLE_NAME)


def get_ws_endpoint():
    """
    Converts:
        wss://xxx.execute-api.region.amazonaws.com/dev
    Into:
        https://xxx.execute-api.region.amazonaws.com/dev
    """
    raw_ep = WEBSOCKET_ENDPOINT

    clean_ep = (
        raw_ep.replace("wss://", "")
              .replace("https://", "")
              .rstrip("/")
    )

    return f"https://{clean_ep}"


apigateway = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=get_ws_endpoint()
)

def format_event(event):
    return {
        "action": "sendMessage",
        "data": event
    }

def lambda_handler(event, context):
    """
    Unified handler for all WebSocket routes: $connect, $disconnect, sendMessage
    """
    # event = format_event(event)
    try:
        route_key = event['requestContext']['routeKey']
        connection_id = event['requestContext']['connectionId']
        
        print(f"Route: {route_key}, Connection ID: {connection_id}")
        
        if route_key == '$connect':
            return handle_connect(event, connection_id)
        elif route_key == '$disconnect':
            return handle_disconnect(event, connection_id)
        elif route_key == 'sendMessage':
            return handle_send_message(event, connection_id)
        else:
            return {
                'statusCode': 400,
                'body': json.dumps({'message': f'Unknown route: {route_key}'})
            }
            
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Internal server error: {str(e)}'})
        }

def handle_connect(event, connection_id):
    """
    Handle WebSocket connection establishment
    Store connection_id in Users table based on session_id
    """
    try:
        # Extract session_id from query parameters
        query_params = event.get('queryStringParameters')
        
        # Handle case where queryStringParameters might be None
        if not query_params:
            print("No query parameters provided")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'session_id query parameter is required'})
            }
            
        session_id = query_params.get('session_id')
        
        if not session_id:
            print("Missing session_id in connection request")
            print(f"Available query params: {query_params}")
            return {
                'statusCode': 400,
                'body': json.dumps({'message': 'session_id query parameter is required'})
            }
        
        print(f"Connecting session_id: {session_id} with connection_id: {connection_id}")
        
        # Find user with matching session_id and update connection_id
        response = users_table.scan(
            FilterExpression='session_id = :session_id',
            ExpressionAttributeValues={':session_id': session_id}
        )
        
        items = response.get('Items', [])
        if not items:
            print(f"No user found with session_id: {session_id}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'User with session_id not found'})
            }
        
        # Update the first matching user with connection_id
        user = items[0]
        email = user['email']
        
        users_table.update_item(
            Key={'email': email},
            UpdateExpression='SET connection_id = :conn_id',
            ExpressionAttributeValues={':conn_id': connection_id}
        )
        
        print(f"Connection established for email: {email}, session: {session_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Connected successfully',
                'session_id': session_id,
                'connection_id': connection_id
            })
        }
        
    except Exception as e:
        print(f"Error in handle_connect: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Connection failed: {str(e)}'})
        }

def handle_disconnect(event, connection_id):
    """
    Handle WebSocket disconnection
    Remove connection_id from Users table
    """
    try:
        # Find user with this connection_id and remove it
        response = users_table.scan(
            FilterExpression='connection_id = :conn_id',
            ExpressionAttributeValues={':conn_id': connection_id}
        )
        
        items = response.get('Items', [])
        for item in items:
            email = item['email']
            users_table.update_item(
                Key={'email': email},
                UpdateExpression='REMOVE connection_id'
            )
            print(f"Disconnected connection_id: {connection_id} for email: {email}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Disconnected successfully'})
        }
        
    except Exception as e:
        print(f"Error in handle_disconnect: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Disconnect failed: {str(e)}'})
        }

def handle_send_message(event, connection_id):
    """
    Handle sendMessage route - process tier completion data and send to client
    This will receive the tier completion data from Step Function
    """
    try:
        # Parse the message body to get tier completion data
        body = event.get('body', '{}')
        if isinstance(body, str):
            message_data = json.loads(body)
        else:
            message_data = body
        print(f"Received message data: {json.dumps(message_data, indent=2)}")
        # Extract the data array from the message
        if 'data' in message_data and isinstance(message_data['data'], list):
            tier_data = message_data['data']
        else:
            # If the body is already the data array
            tier_data = message_data if isinstance(message_data, list) else [message_data]
        print(f"Processing tier data: {json.dumps(tier_data, indent=2)}")
        processed_items = []
        # Process each item in the tier data
        for item in tier_data:
            try:
                # Skip if item doesn't have the expected structure
                if not isinstance(item, dict):
                    print(f"Skipping invalid item: {item}")
                    continue
                processed_item = process_tier_item(item, connection_id)
                if processed_item:
                    processed_items.append(processed_item)
            except Exception as e:
                print(f"Error processing item: {str(e)}")
                continue
        return event
    except Exception as e:
        print(f"Error in handle_send_message: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'message': f'Send message failed: {str(e)}'})
        }

        
def process_tier_item(item, connection_id=None):
    """
    Process individual tier item, fetch content from S3, and send via WebSocket
    """
    try:
        # Extract required fields
        session_id = item.get('session_id')
        project_id = item.get('project_id')
        user_id = item.get('user_id')
        document_type = item.get('document_type')
        key = item.get('key')
        tier = item.get('tier')
        status = item.get('status')
        
        print(f"Processing item - Session: {session_id}, Project: {project_id}, Key: {key}")
        
        if not all([session_id, project_id, document_type, key]):
            print(f"Missing required fields in item: {item}")
            return None
        
        # Split the key to get folder and filename
        try:
            if '/' in key:
                folder, filename = key.split('/', 1)  # Split only on first '/'
            else:
                folder = key
                filename = key
        except ValueError:
            folder = key
            filename = key
            
        print(f"Parsed key - Folder: {folder}, Filename: {filename}")
        
        # Fetch content from S3
        content_data = fetch_content_from_s3(project_id, document_type, key, filename)
        
        # Find connection_id if not provided
        if not connection_id:
            connection_id = find_connection_by_session(session_id)
        
        if connection_id:
            # Prepare message for WebSocket
            message = {
                'type': 'tier_completion',
                'data': {
                    'session_id': session_id,
                    'project_id': project_id,
                    'user_id': user_id,
                    'document_type': document_type,
                    'key': key,
                    'tier': tier,
                    'status': status,
                    'result': item.get('result', {}),
                    'content': content_data,
                    'timestamp': context.aws_request_id if 'context' in globals() else 'unknown'
                }
            }
            
            # Send message via WebSocket
            send_result = send_websocket_message(connection_id, message)
            
            return {
                'key': key,
                'tier': tier,
                'status': 'sent' if send_result else 'failed',
                'connection_id': connection_id,
                'content_fetched': content_data.get('content') is not None
            }
        else:
            print(f"No active connection found for session_id: {session_id}")
            return {
                'key': key,
                'tier': tier,
                'status': 'no_connection',
                'session_id': session_id
            }
            
    except Exception as e:
        print(f"Error processing tier item: {str(e)}")
        return {
            'key': item.get('key', 'unknown'),
            'tier': item.get('tier', 'unknown'),
            'status': 'error',
            'error': str(e)
        }

def fetch_content_from_s3(project_id, document_type, key, filename):
    """
    Fetch content from S3 using the specified path pattern
    """
    try:
        # Construct S3 key: {project_id}/{document_type}/output/{key}/{filename}.txt
        s3_key = f"{project_id}/{document_type}/output/{key}/{filename}.txt"
        
        print(f"Fetching from S3 - Bucket: {bucket_name}, Key: {s3_key}")
        
        # Fetch content from S3
        response = s3.get_object(Bucket=bucket_name, Key=s3_key)
        content = response['Body'].read().decode('utf-8')
        
        return {
            's3_key': s3_key,
            'content': content,
            'content_length': len(content),
            'status': 'success'
        }
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"S3 ClientError: {error_code} for key: {s3_key}")
        
        if error_code == 'NoSuchKey':
            return {
                's3_key': s3_key,
                'content': None,
                'error': 'File not found in S3',
                'status': 'not_found'
            }
        else:
            return {
                's3_key': s3_key,
                'content': None,
                'error': f'S3 Error: {str(e)}',
                'status': 'error'
            }
            
    except Exception as e:
        print(f"Unexpected error fetching from S3: {str(e)}")
        return {
            's3_key': f"{project_id}/{document_type}/output/{key}/{filename}.txt",
            'content': None,
            'error': f'Unexpected error: {str(e)}',
            'status': 'error'
        }

def find_connection_by_session(session_id):
    """
    Find active WebSocket connection by session_id
    """
    try:
        # Scan Users table to find user with matching session_id
        response = users_table.scan(
            FilterExpression='session_id = :session_id AND attribute_exists(connection_id)',
            ExpressionAttributeValues={':session_id': session_id}
        )
        
        items = response.get('Items', [])
        if items:
            user = items[0]
            connection_id = user.get('connection_id')
            
            if connection_id:
                print(f"Found connection_id {connection_id} for session_id {session_id}")
                return connection_id
            
        print(f"No active connection found for session_id: {session_id}")
        return None
        
    except Exception as e:
        print(f"Error finding connection: {str(e)}")
        return None

def send_websocket_message(connection_id, message):
    """
    Send message to WebSocket connection
    """
    try:
        apigateway.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(message, default=str)
        )
        print(f"Message sent successfully to connection: {connection_id}")
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'GoneException':
            print(f"Connection {connection_id} is no longer available - cleaning up")
            cleanup_stale_connection(connection_id)
        else:
            print(f"Error sending WebSocket message: {str(e)}")
        return False
        
    except Exception as e:
        print(f"Unexpected error sending message: {str(e)}")
        return False

def cleanup_stale_connection(connection_id):
    """
    Clean up stale WebSocket connection from Users table
    """
    try:
        response = users_table.scan(
            FilterExpression='connection_id = :conn_id',
            ExpressionAttributeValues={':conn_id': connection_id}
        )
        
        for item in response.get('Items', []):
            users_table.update_item(
                Key={'email': item['email']},
                UpdateExpression='REMOVE connection_id'
            )
            print(f"Cleaned up stale connection {connection_id} for user {item['email']}")
            
    except Exception as e:
        print(f"Error cleaning up stale connection: {str(e)}")