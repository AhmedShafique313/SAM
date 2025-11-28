import json
import boto3
import os
from botocore.exceptions import ClientError

# -------------------------------------------------------
#  HARDCODED RESOURCES
# -------------------------------------------------------
USERS_TABLE_NAME = "users-table"
BUCKET_NAME = "cammi-devprod"
WEBSOCKET_ENDPOINT = os.environ["WEBSOCKET_ENDPOINT1"]  # dynamic from CFN


dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

users_table = dynamodb.Table(USERS_TABLE_NAME)


# -------------------------------------------------------
# NORMALIZE WEBSOCKET ENDPOINT
# -------------------------------------------------------

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


# -------------------------------------------------------
# MAIN HANDLER
# -------------------------------------------------------

def lambda_handler(event, context):
    try:
        route_key = event["requestContext"]["routeKey"]
        connection_id = event["requestContext"]["connectionId"]

        print(f"[WS] Route={route_key}   CID={connection_id}")

        if route_key == "$connect":
            return handle_connect(event, connection_id)

        elif route_key == "$disconnect":
            return handle_disconnect(connection_id)

        elif route_key == "realtimetext":
            return handle_message(event, connection_id)

        return {"statusCode": 400, "body": "Unknown route"}

    except Exception as e:
        print("Handler ERROR:", e)
        return {"statusCode": 500, "body": str(e)}


# -------------------------------------------------------
# CONNECT
# -------------------------------------------------------

def handle_connect(event, connection_id):

    try:
        query = event.get("queryStringParameters") or {}
        session_id = query.get("session_id")

        if not session_id:
            return {"statusCode": 400, "body": "session_id required"}

        print(f"[CONNECT] session_id={session_id} CID={connection_id}")

        # Find user with matching session_id
        resp = users_table.scan(
            FilterExpression="session_id = :sid",
            ExpressionAttributeValues={":sid": session_id}
        )

        items = resp.get("Items", [])
        if not items:
            return {"statusCode": 404, "body": "Invalid session_id"}

        email = items[0]["email"]

        # Update user record with connection_id
        users_table.update_item(
            Key={"email": email},
            UpdateExpression="SET connection_id = :cid",
            ExpressionAttributeValues={":cid": connection_id}
        )

        print(f"[CONNECT] connection stored for {email}")

        return {"statusCode": 200}

    except Exception as e:
        print("CONNECT ERROR:", e)
        return {"statusCode": 500, "body": str(e)}


# -------------------------------------------------------
# DISCONNECT
# -------------------------------------------------------

def handle_disconnect(connection_id):
    try:
        print(f"[DISCONNECT] CID={connection_id}")

        resp = users_table.scan(
            FilterExpression="connection_id = :cid",
            ExpressionAttributeValues={":cid": connection_id}
        )

        for item in resp.get("Items", []):
            users_table.update_item(
                Key={"email": item["email"]},
                UpdateExpression="REMOVE connection_id"
            )

        return {"statusCode": 200}

    except Exception as e:
        print("DISCONNECT ERROR:", e)
        return {"statusCode": 500, "body": str(e)}


# -------------------------------------------------------
# INCOMING MESSAGE
# -------------------------------------------------------

def handle_message(event, connection_id):
    try:
        body = json.loads(event.get("body", "{}"))
        print("[RECV]", body)

        # Echo back received message
        response = {
            "type": "echo",
            "received": body
        }

        send_ws(connection_id, response)

        return {"statusCode": 200}

    except Exception as e:
        print("MESSAGE ERROR:", e)
        return {"statusCode": 500, "body": str(e)}


# -------------------------------------------------------
# SEND WEBSOCKET MESSAGE
# -------------------------------------------------------

def send_ws(connection_id, data):
    try:
        apigateway.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data)
        )
        print(f"[WS SEND] -> {connection_id}")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "GoneException":
            print("[STALE] Connection is gone → cleaning", connection_id)
            cleanup_stale_connection(connection_id)
        else:
            print("[WS SEND ERROR]", e)
        return False


# -------------------------------------------------------
# CLEANUP STALE CONNECTION
# -------------------------------------------------------

def cleanup_stale_connection(connection_id):
    try:
        resp = users_table.scan(
            FilterExpression="connection_id = :cid",
            ExpressionAttributeValues={":cid": connection_id}
        )

        for item in resp.get("Items", []):
            users_table.update_item(
                Key={"email": item["email"]},
                UpdateExpression="REMOVE connection_id"
            )

    except Exception as e:
        print("CLEANUP ERROR:", e)
