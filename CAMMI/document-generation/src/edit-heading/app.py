import json
import boto3
import os
from boto3.dynamodb.conditions import Attr

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')

USERS_TABLE = os.environ.get('USERS_TABLE', 'users')
PROJECT_STATE_TABLE = "project-state-table"

users_table = dynamodb.Table(USERS_TABLE)
project_state_table = dynamodb.Table(PROJECT_STATE_TABLE)

stepfunctions = boto3.client('stepfunctions')

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def lambda_handler(event, context):
    try:
        route_key = event.get("requestContext", {}).get("routeKey")

        # 🔹 Case 1: $connect
        if route_key == "$connect":
            connection_id = event["requestContext"]["connectionId"]
            query_params = event.get("queryStringParameters", {}) or {}
            session_id = query_params.get("session_id")

            if not session_id:
                return {"statusCode": 400, "body": "session_id query param required"}

            response = users_table.scan(
                FilterExpression=Attr('session_id').eq(session_id),
                ProjectionExpression='id, email'
            )

            if not response.get('Items'):
                return {"statusCode": 404, "body": "User not found for given session_id"}

            user_item = response['Items'][0]
            user_id = user_item['id']

            key = {"email": user_item['email']} if 'email' in user_item else {"id": user_id}

            users_table.update_item(
                Key=key,
                UpdateExpression="set connection_id = :c",
                ExpressionAttributeValues={":c": connection_id}
            )

            return {"statusCode": 200, "body": f"Connected. session_id={session_id} stored."}

        # 🔹 Case 2: $disconnect
        elif route_key == "$disconnect":
            connection_id = event["requestContext"]["connectionId"]

            user_resp = users_table.scan(
                FilterExpression=Attr('connection_id').eq(connection_id),
                ProjectionExpression='id, email'
            )

            if user_resp.get('Items'):
                user_item = user_resp['Items'][0]
                key = {"email": user_item['email']} if 'email' in user_item else {"id": user_item['id']}

                users_table.update_item(
                    Key=key,
                    UpdateExpression="REMOVE connection_id"
                )

            return {"statusCode": 200, "body": "Disconnected and connection_id removed."}

        # 🔹 Case 3: editHeading
        elif route_key == "editHeading":
            body = json.loads(event.get("body", "{}"))

            session_id = body.get("session_id")
            project_id = body.get("project_id")
            heading = body.get("heading")
            subheading = body.get("subheading")
            prompt = body.get("prompt")

            if not all([session_id, project_id, heading, subheading, prompt]):
                return {"statusCode": 400, "body": json.dumps({"error": "Missing required fields"})}

            # ✅ Fetch user info
            user_resp = users_table.scan(
                FilterExpression=Attr('session_id').eq(session_id),
                ProjectionExpression="id, connection_id"
            )

            if not user_resp.get('Items'):
                return {"statusCode": 404, "body": json.dumps({"error": "User not found"})}

            user = user_resp['Items'][0]
            user_id = user['id']
            connection_id = user.get('connection_id')

            # ✅ NEW: Fetch active_document from project-state-table
            project_state_resp = project_state_table.get_item(
                Key={"project_id": project_id}
            )

            if "Item" not in project_state_resp:
                return {
                    "statusCode": 404,
                    "body": json.dumps({"error": "Project state not found"})
                }

            active_document = project_state_resp["Item"].get("active_document")

            if not active_document or not isinstance(active_document, str):
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "active_document not set or invalid in project-state-table"})
                }

            document_type = active_document.lower()

            if not document_type:
                return {
                    "statusCode": 400,
                    "body": json.dumps({"error": "active_document not set for this project"})
                }

            # Step Function input remains same
            step_input = {
                "heading": heading,
                "subheading": subheading,
                "prompt": prompt,
                "session_id": session_id,
                "user_id": user_id,
                "project_id": project_id,
                "document_type": document_type,
                "connection_id": connection_id
            }

            response = stepfunctions.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                input=json.dumps(step_input)
            )

            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Step Function triggered successfully",
                    "executionArn": response["executionArn"]
                })
            }

        # 🔹 Default: Other routes
        else:
            return {"statusCode": 200, "body": f"Unhandled route: {route_key}"}

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
