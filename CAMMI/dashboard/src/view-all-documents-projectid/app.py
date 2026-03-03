import json
import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb')
DOCUMENT_HISTORY_TABLE = "documents-history-table"
PROJECT_GSI_NAME = "project-id-doc-index"  # Your GSI name

def lambda_handler(event, context):
    # ✅ Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return _cors_response(200, {})

    try:
        # ✅ Parse input
        body = json.loads(event.get("body", "{}"))
        project_id = body.get("project_id")
        if not project_id:
            return _error_response("Missing project_id in request body")

        doc_table = dynamodb.Table(DOCUMENT_HISTORY_TABLE)

        # ✅ Query documents using project_id GSI
        documents = []
        response = doc_table.query(
            IndexName=PROJECT_GSI_NAME,
            KeyConditionExpression=Key("project_id").eq(project_id),
            Limit=50  # optional, control page size
        )
        documents.extend(response.get("Items", []))

        # ✅ Handle pagination if more results exist
        while "LastEvaluatedKey" in response:
            response = doc_table.query(
                IndexName=PROJECT_GSI_NAME,
                KeyConditionExpression=Key("project_id").eq(project_id),
                Limit=50,
                ExclusiveStartKey=response["LastEvaluatedKey"]
            )
            documents.extend(response.get("Items", []))

        return _cors_response(200, {
            "project_id": project_id,
            "documents": documents
        })

    except Exception as e:
        # Log error in CloudWatch
        print(f"Error fetching documents for project {project_id}: {str(e)}")
        return _error_response("Failed to fetch documents")


def _cors_response(status_code, body_dict):
    """Return response with proper CORS headers"""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",  # Change to frontend domain in production
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Content-Type": "application/json"
        },
        "body": json.dumps(body_dict)
    }


def _error_response(message, code=400):
    """Return error response with CORS headers"""
    return _cors_response(code, {"error": message})