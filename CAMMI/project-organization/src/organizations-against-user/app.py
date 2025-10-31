import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")          
org_table = dynamodb.Table("organizations-table")   
proj_table = dynamodb.Table("projects-table")      


def lambda_handler(event, context):
    try:
        # --- Step 1: Scan all tables ---
        users_response = users_table.scan()
        org_response = org_table.scan()
        proj_response = proj_table.scan()

        users = users_response.get("Items", [])
        organizations = org_response.get("Items", [])
        projects = proj_response.get("Items", [])

        # --- Step 2: Group projects by organization_id ---
        projects_by_org = {}
        for proj in projects:
            org_id = proj.get("organization_id")
            if org_id not in projects_by_org:
                projects_by_org[org_id] = []
            projects_by_org[org_id].append(proj)

        # --- Step 3: Group organizations by user_id ---
        orgs_by_user = {}
        for org in organizations:
            user_id = org.get("user_id")
            if user_id not in orgs_by_user:
                orgs_by_user[user_id] = []
            # Attach projects to the organization
            org_with_projects = {
                "id": org.get("id"),
                "organization_name": org.get("organization_name"),
                "createdAt": org.get("createdAt"),
                "projects": projects_by_org.get(org.get("id"), [])
            }
            orgs_by_user[user_id].append(org_with_projects)

        # --- Step 4: Attach organizations to users ---
        enriched_users = []
        for user in users:
            user_orgs = orgs_by_user.get(user.get("id"), [])
            enriched_users.append({
                "email": user.get("email"),
                "id": user.get("id"),
                "name": user.get("name"),
                "firstName": user.get("firstName"),
                "lastName": user.get("lastName"),
                "createdAt": user.get("createdAt"),
                "organizations": user_orgs
            })

        # --- Step 5: Return Response ---
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "total_users": len(enriched_users),
                "users": enriched_users
            })
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({"error": str(e)})
        }
