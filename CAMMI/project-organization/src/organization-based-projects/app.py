import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
org_table = dynamodb.Table("organizations-table")  
proj_table = dynamodb.Table("projects-table")       


def lambda_handler(event, context):
    try:
        # Scan both tables
        org_response = org_table.scan()
        proj_response = proj_table.scan()

        organizations = org_response.get("Items", [])
        projects = proj_response.get("Items", [])

        # Build dictionary of projects grouped by organization_id
        projects_by_org = {}
        for proj in projects:
            org_id = proj.get("organization_id")
            if org_id not in projects_by_org:
                projects_by_org[org_id] = []
            projects_by_org[org_id].append(proj)

        # Attach projects to their organizations
        enriched_orgs = []
        for org in organizations:
            org_id = org.get("id")
            org_projects = projects_by_org.get(org_id, [])
            enriched_orgs.append({
                "id": org_id,
                "organization_name": org.get("organization_name"),
                "createdAt": org.get("createdAt"),
                "user_id": org.get("user_id"),
                "projects": org_projects
            })

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type"
            },
            "body": json.dumps({
                "total_organizations": len(enriched_orgs),
                "organizations": enriched_orgs
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
