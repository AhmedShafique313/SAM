import boto3
import json
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
users_table = dynamodb.Table('users-table')
review_table = dynamodb.Table('review-documents-table')


def lambda_handler(event, context):
    try:
        # --- Step 1: Extract session_id ---
        session_id = None

        # Case 1: Direct invocation (Lambda test)
        if "session_id" in event:
            session_id = event["session_id"]

        # Case 2: Invoked through API Gateway (body as JSON string)
        elif "body" in event and event["body"]:
            try:
                body = json.loads(event["body"])
                session_id = body.get("session_id")
            except json.JSONDecodeError:
                pass

        if not session_id:
            return cors_response(400, {"message": "session_id is required"})

        # --- Step 2: Query Users table by session_id ---
        user_response = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id)
        )

        if not user_response.get("Items"):
            return cors_response(404, {"message": "User not found for given session_id"})

        user = user_response["Items"][0]
        user_id = user["id"]

        # --- Step 3: Query ReviewDocument table by user_id ---
        review_response = review_table.query(
            IndexName="user_id-index",
            KeyConditionExpression=Key("user_id").eq(user_id)
        )

        items = review_response.get("Items", [])

        # --- Step 4: Format output ---
        reviews = []
        for idx, doc in enumerate(items, start=1):
            reviews.append({
                "No": idx,
                "DocumentName": doc.get("document_type", ""),
                "Organization": doc.get("organization_name", ""),
                "Date": doc.get("createdAt", ""),
                "Project": doc.get("project_name", ""),
                "Status": doc.get("status", ""),
                "project_id": doc.get("project_id", ""),
                "document_type_uuid": doc.get("document_type_uuid", "")
            })

        return cors_response(200, reviews)

    except Exception as e:
        print("Error:", str(e))
        return cors_response(500, {"message": f"Error: {str(e)}"})


def cors_response(status_code, body):
    """Helper to add full CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,session_id",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Credentials": "true"
        },
        "body": json.dumps(body)
    }
