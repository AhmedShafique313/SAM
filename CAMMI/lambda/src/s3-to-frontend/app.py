import json
import base64
import boto3
import os
from boto3.dynamodb.conditions import Attr

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

USERS_TABLE = os.environ.get('USERS_TABLE', 'users-table')
PROJECT_STATE_TABLE = "project-state-table"

users_table = dynamodb.Table(USERS_TABLE)
project_state_table = dynamodb.Table(PROJECT_STATE_TABLE)

BUCKET_NAME = "cammi-devprod"

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST, OPTIONS"
}


def lambda_handler(event, context):
    try:
        # ---------------------------------
        # Handle CORS preflight
        # ---------------------------------
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": ""
            }

        # ---------------------------------
        # Extract body (POST request)
        # ---------------------------------
        body = event.get("body")

        if body and isinstance(body, str):
            body = json.loads(body)

        session_id = body.get("session_id") if body else None
        project_id = body.get("project_id") if body else None

        if not session_id:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "session_id is required"})
            }

        if not project_id:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "project_id is required"})
            }

        # ---------------------------------
        # Find user using session_id
        # ---------------------------------
        user_resp = users_table.scan(
            FilterExpression=Attr('session_id').eq(session_id),
            ProjectionExpression="id"
        )

        if not user_resp.get("Items"):
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "error": "User not found for the given session_id"
                })
            }

        user_id = user_resp["Items"][0]["id"]

        # ---------------------------------
        # Get active_document from project-state-table
        # ---------------------------------
        project_state_resp = project_state_table.get_item(
            Key={"project_id": project_id}
        )

        if "Item" not in project_state_resp:
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "error": "Project state not found for given project_id"
                })
            }

        document_type = project_state_resp["Item"].get("generating_document").lower()

        if not document_type:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "error": "generating_document not set for this project"
                })
            }

        # ---------------------------------
        # Construct S3 folder path
        # ---------------------------------
        folder_prefix = f"project/{project_id}/{document_type}/marketing_strategy_document/"

        # ---------------------------------
        # List S3 objects
        # ---------------------------------
        response = s3.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=folder_prefix
        )

        docx_files = [
            obj for obj in response.get("Contents", [])
            if obj["Key"].lower().endswith(".docx")
        ]

        if not docx_files:
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "error": "No .docx files found in the folder"
                })
            }

        # ---------------------------------
        # Get latest file
        # ---------------------------------
        latest_file = max(docx_files, key=lambda x: x["LastModified"])
        latest_key = latest_file["Key"]

        # ---------------------------------
        # Read and encode file
        # ---------------------------------
        s3_response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=latest_key
        )

        file_data = s3_response["Body"].read()
        encoded_data = base64.b64encode(file_data).decode("utf-8")

        # ---------------------------------
        # Success Response
        # ---------------------------------
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Latest .docx file fetched successfully",
                "user_id": user_id,
                "project_id": project_id,
                "document_type": document_type,
                "fileName": latest_key.split("/")[-1],
                "docxBase64": encoded_data
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "error": str(e)
            })
        }
