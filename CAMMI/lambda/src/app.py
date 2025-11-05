import json
import base64
import boto3
import os
from boto3.dynamodb.conditions import Attr
 
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
USERS_TABLE = os.environ.get('USERS_TABLE', 'users-table')
users_table = dynamodb.Table(USERS_TABLE)
BUCKET_NAME = "cammi-devprod"
 
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, session_id",
    "Access-Control-Allow-Methods": "GET, OPTIONS"
}
 
def lambda_handler(event, context):
    try:
        # Handle preflight OPTIONS request
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": CORS_HEADERS,
                "body": ""
            }
 
        # Get headers
        headers = event.get('headers', {})
 
        # Get session_id from header
        session_id = headers.get('session_id')
        # Get document_type from header
        document_type = headers.get('document_type')
        # Get project_id from header
        project_id = headers.get('project_id')
        if not session_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "session_id is required"}),
                "headers": CORS_HEADERS
            }
 
        # Find user by session_id
        user_resp = users_table.scan(
            FilterExpression=Attr('session_id').eq(session_id),
            ProjectionExpression="id"
        )
 
        if 'Items' not in user_resp or len(user_resp['Items']) == 0:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "User not found for the given session_id"}),
                "headers": CORS_HEADERS
            }
 
        user_id = user_resp['Items'][0]['id']
 
        # Folder path for the user
        FOLDER_PREFIX = f"project/{project_id}/{document_type}/marketing_strategy_document/"
        
 
        # List objects in S3 folder
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=FOLDER_PREFIX)
 
        # Filter only .docx files
        docx_files = [
            obj for obj in response.get("Contents", [])
            if obj["Key"].endswith(".docx")
        ]
        if not docx_files:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "No .docx files found in the folder"}),
                "headers": CORS_HEADERS
            }
 
        # Get latest file
        latest_file = max(docx_files, key=lambda x: x["LastModified"])
        latest_key = latest_file["Key"]
 
        # Read and encode file
        s3_response = s3.get_object(Bucket=BUCKET_NAME, Key=latest_key)
        file_data = s3_response["Body"].read()
        encoded_data = base64.b64encode(file_data).decode("utf-8")
 
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Latest .docx file fetched successfully",
                "fileName": latest_key.split("/")[-1],
                "docxBase64": encoded_data
            }),
            "headers": CORS_HEADERS
        }
 
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": CORS_HEADERS
        }