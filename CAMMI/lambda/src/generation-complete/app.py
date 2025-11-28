import boto3, os
import json
from boto3.dynamodb.conditions import Attr

# ----------------------------
# Normalize WebSocket endpoint
# ----------------------------
raw_endpoint = os.environ["WEBSOCKET_ENDPOINT"]

# Clean and normalize the endpoint
clean_endpoint = (
    raw_endpoint
    .replace("wss://", "")
    .replace("https://", "")
    .rstrip("/")
)

# Final correct endpoint for ApiGatewayManagementApi
final_endpoint = "https://" + clean_endpoint
print("Using WebSocket endpoint:", final_endpoint)

# DynamoDB client
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")  # change to your actual table name

# WebSocket API Gateway client
apigw = boto3.client(
    "apigatewaymanagementapi",
    endpoint_url=final_endpoint
)


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

    # Message to send to WebSocket client
    message = {
        "action": "sendMessage",
        "body": "Document generated successfully!"
    }

    try:
        apigw.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(message).encode("utf-8")
        )
        print(f"Message sent to connection {connection_id}")
    except Exception as e:
        print(f"Error sending message: {str(e)}")
        return {"statusCode": 500, "body": f"Error: {str(e)}"}

    return {
        "statusCode": 200,
        "body": f"Message sent to client {connection_id}"
    }
