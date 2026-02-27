import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")

USERS_TABLE = dynamodb.Table("users-table")
PROJECTS_TABLE = dynamodb.Table("projects-table")
DOCUMENTS_TABLE = dynamodb.Table("documents-history-table")

USERS_SESSION_INDEX = "session_id-index"
PROJECTS_USER_INDEX = "user_id-index"
PROJECT_DOCUMENTS_INDEX = "project-id-doc-index"  # Your GSI


def lambda_handler(event, context):
    try:
        # ----------------------------------
        # 1. Parse POST body
        # ----------------------------------
        body = event.get("body")
        if not body:
            return response(400, {"error": "Request body is required"})

        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            return response(400, {"error": "Invalid JSON format"})

        session_id = body.get("session_id")
        if not session_id:
            return response(400, {"error": "session_id is required"})

        # ----------------------------------
        # 2. Get user_id from users-table
        # ----------------------------------
        user_response = USERS_TABLE.query(
            IndexName=USERS_SESSION_INDEX,
            KeyConditionExpression=Key("session_id").eq(session_id)
        )

        if not user_response["Items"]:
            return response(404, {"error": "Invalid session_id"})

        user_item = user_response["Items"][0]
        user_id = user_item.get("user_id") or user_item.get("id")

        if not user_id:
            return response(500, {"error": "user_id not found for session"})

        # ----------------------------------
        # 3. Fetch projects for this user
        # ----------------------------------
        projects = []
        last_evaluated_key = None

        while True:
            query_params = {
                "IndexName": PROJECTS_USER_INDEX,
                "KeyConditionExpression": Key("user_id").eq(user_id)
            }

            if last_evaluated_key:
                query_params["ExclusiveStartKey"] = last_evaluated_key

            proj_response = PROJECTS_TABLE.query(**query_params)
            projects.extend(proj_response.get("Items", []))

            last_evaluated_key = proj_response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                break

        # ----------------------------------
        # 4. Add document count for each project
        # ----------------------------------
        filtered_projects = []
        for project in projects:
            project_id = project.get("id")
            project_name = project.get("project_name")

            # Query documents-history-table GSI for count
            doc_count = 0
            try:
                response_doc = DOCUMENTS_TABLE.query(
                    IndexName=PROJECT_DOCUMENTS_INDEX,
                    KeyConditionExpression=Key("project_id").eq(project_id),
                    Select="COUNT"
                )
                doc_count = response_doc.get("Count", 0)
            except ClientError as e:
                print(f"Error counting documents for project {project_id}: {e}")

            filtered_projects.append({
                "projectId": project_id,
                "projectName": project_name,
                "documentCount": doc_count
            })

        # ----------------------------------
        # 5. Success response
        # ----------------------------------
        return response(200, {
            "session_id": session_id,
            "user_id": user_id,
            "projects": filtered_projects
        })

    except ClientError as e:
        return response(500, {
            "error": "DynamoDB operation failed",
            "details": e.response["Error"]["Message"]
        })

    except Exception as e:
        return response(500, {
            "error": "Internal server error",
            "details": str(e)
        })


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",

            # ✅ CORS
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body, default=str)
    }