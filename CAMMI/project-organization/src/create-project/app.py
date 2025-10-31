import json
import boto3
import uuid
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")

# Tables
USERS_TABLE = "users-table"
ORGANIZATIONS_TABLE = "organizations-table"
PROJECTS_TABLE = "projects-table"

users_table = dynamodb.Table(USERS_TABLE)
organizations_table = dynamodb.Table(ORGANIZATIONS_TABLE)
projects_table = dynamodb.Table(PROJECTS_TABLE)


def lambda_handler(event, context):
    try:
        body = json.loads(event["body"]) if "body" in event else event
        session_id = body.get("session_id")
        organization_name = body.get("organization_name")
        project_name = body.get("project_name")

        if not session_id or not organization_name or not project_name:
            return response(400, {"error": "Missing required fields"})

        # 1. Find user from Users table using session_id
        user_resp = users_table.scan(
            FilterExpression=Attr("session_id").eq(session_id)
        )

        if not user_resp["Items"]:
            return response(404, {"error": "User not found with given session_id"})

        user = user_resp["Items"][0]
        user_id = user["id"]

        # 2. Check if organization exists for this user
        org_resp = organizations_table.scan(
            FilterExpression=Attr("user_id").eq(user_id) & Attr("organization_name").eq(organization_name)
        )

        if org_resp["Items"]:
            org_id = org_resp["Items"][0]["id"]
        else:
            # Create new organization with default post_question_flag = True
            org_id = str(uuid.uuid4())
            organizations_table.put_item(
                Item={
                    "id": org_id,
                    "organization_name": organization_name,
                    "user_id": user_id,
                    "createdAt": datetime.utcnow().isoformat(),
                    "post_question_flag": True  # ✅ Added default column
                }
            )

        # 3. Check if project exists under this organization
        proj_resp = projects_table.scan(
            FilterExpression=Attr("organization_id").eq(org_id) & Attr("project_name").eq(project_name)
        )

        if proj_resp["Items"]:
            return response(400, {"error": "Project with this name already exists in the organization"})

        # Create new project
        proj_id = str(uuid.uuid4())
        projects_table.put_item(
            Item={
                "id": proj_id,
                "project_name": project_name,
                "organization_id": org_id,
                "createdAt": datetime.utcnow().isoformat()
            }
        )

        return response(200, {
            "message": "Project created successfully",
            "organization_id": org_id,
            "project_id": proj_id
        })

    except Exception as e:
        return response(500, {"error": str(e)})


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }