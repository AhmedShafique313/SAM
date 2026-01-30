import json
import uuid
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")

PROJECTS_TABLE_NAME = "projects-table"
USERS_TABLE_NAME = "users-table"

projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)


def lambda_handler(event, context):
    try:
        # -------------------------------
        # 1. Parse input from API Gateway
        # -------------------------------
        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_name = body.get("project_name")

        if not session_id or not project_name:
            return response(400, "session_id and project_name are required")

        project_name = project_name.strip()

        if len(project_name) < 3:
            return response(400, "project_name must be at least 3 characters")

        # ------------------------------------------------
        # 2. Get user_id from users-table using session_id
        # ------------------------------------------------
        user_lookup = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id)
        )

        if not user_lookup["Items"]:
            return response(404, "User not found for given session_id")

        user_id = user_lookup["Items"][0]["id"]

        # -----------------------------------------
        # 3. Check for duplicate project_name
        # -----------------------------------------
        duplicate_check = projects_table.query(
            IndexName="project_name-index",
            KeyConditionExpression=Key("project_name").eq(project_name)
        )

        if duplicate_check["Items"]:
            return response(409, "Project name already exists")

        # -----------------------------------------
        # 4. Create project record
        # -----------------------------------------
        project_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat()

        item = {
            "id": project_id,
            "createdAt": created_at,
            "organization_id": session_id,
            "user_id": user_id,
            "project_name": project_name
        }

        # -----------------------------------------
        # 5. Put item with safety condition
        # -----------------------------------------
        projects_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(id)"
        )

        # -----------------------------------------
        # 6. Return success response
        # -----------------------------------------
        return response(201, {
            "project_id": project_id,
            "project_name": project_name,
            "user_id": user_id
        })

    except projects_table.meta.client.exceptions.ConditionalCheckFailedException:
        return response(409, "Project already exists")

    except Exception as e:
        return response(500, str(e))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body)
    }
