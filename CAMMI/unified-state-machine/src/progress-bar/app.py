import boto3
import json
from boto3.dynamodb.conditions import Attr
 
# DynamoDB client
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")  # change to your actual table name
 
# WebSocket API Gateway client
apigw = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url="https://4iqvtvmxle.execute-api.us-east-1.amazonaws.com/prod"
)
 
s3 = boto3.client('s3')
bucket_name = 'cammi-devprod'
 
 
def get_tier_completion_percentage(bucket_name, object_key):
    """
    Reads the execution_plan.json from S3,
    counts true/false tiers, and calculates completion percentage.
    """
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        data = json.loads(response['Body'].read())
 
        total_tiers = len(data)
        if total_tiers == 0:
            return {
                "true_count": 0,
                "false_count": 0,
                "completion_percentage": 0.0
            }
 
        true_count = sum(1 for tier in data.values() if tier.get("status") is True)
        false_count = total_tiers - true_count
        completion_percentage = (true_count / total_tiers) * 100
 
        return  round(completion_percentage, 2)
       
 
    except Exception as e:
        return {
            "error": repr(e),
            "true_count": 0,
            "false_count": 0,
            "completion_percentage": 0.0
        }
 
 
def lambda_handler(event, context):
    """
    Step Function triggers this Lambda.
    This Lambda finds the connectionId and sends a message directly
    to the WebSocket client via the sendMessage route.
    """
 
    print("Incoming event:", json.dumps(event))
 
    if not isinstance(event, dict):
        return {"statusCode": 400, "body": "Expected a dictionary"}
 
    session_id = event.get("session_id")
    user_id = event.get("user_id", "")
    if not session_id:
        return {"statusCode": 400, "body": "session_id missing in input"}
 
    # Get connectionId from DynamoDB
    response = users_table.scan(
        FilterExpression=Attr("session_id").eq(session_id)
    )
 
    items = response.get("Items", [])
    if not items:
        return {"statusCode": 404, "body": f"No user found with session_id {session_id}"}
       
     
    connection_id = items[0]["connection_id"]
    document_type = event.get("document_type", "")
   
    # ✅ Determine S3 key
    if document_type:
        object_key = f'flow/{user_id}/{document_type}/execution_plan.json'
    else:
        object_key = 'flow/execution_plan.json'    
 
    # Compute completion stats
    completion_stats = get_tier_completion_percentage(bucket_name, object_key)
   
 
    # ✅ Message we want to send to the WebSocket client
    message = {
        "action": "sendMessage",  # this matches your WebSocket route
        "body": completion_stats
    }
 
    try:
        # Send the message to the WebSocket client
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(message).encode("utf-8")
        )
        print(f"Message sent to connection {connection_id}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}
 
    return event