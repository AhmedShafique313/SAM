import json
import uuid
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
TABLE_NAME = "projects-table"

table = dynamodb.Table(TABLE_NAME)


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

        # -----------------------------------------
        # 2. Check for duplicate project_name
        # -----------------------------------------
        duplicate_check = table.query(
            IndexName="project_name-index",
            KeyConditionExpression=Key("project_name").eq(project_name)
        )

        if duplicate_check["Items"]:
            return response(409, "Project name already exists")

        # -----------------------------------------
        # 3. Create project record
        # -----------------------------------------
        project_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat()

        item = {
            "id": project_id,
            "createdAt": created_at,
            "organization_id": session_id,
            "project_name": project_name
        }

        # -----------------------------------------
        # 4. Put item with safety condition
        # -----------------------------------------
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(id)"
        )

        # -----------------------------------------
        # 5. Return success response
        # -----------------------------------------
        return response(201, {
            "project_id": project_id,
            "project_name": project_name
        })

    except table.meta.client.exceptions.ConditionalCheckFailedException:
        return response(409, "Project already exists")

    except Exception as e:
        return response(500, str(e))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",

            # ✅ CORS headers
            "Access-Control-Allow-Origin": "*",  
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body)
    }
