import json
import boto3
import base64
import uuid
import datetime
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# Tables
users_table = dynamodb.Table("users-table")
org_table = dynamodb.Table("organizations-table")
projects_table = dynamodb.Table("projects-table")
review_table = dynamodb.Table("review-documents-table")

BUCKET_NAME = "cammi"
FOLDER_NAME = "ReviewDocuments"

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(body)
    }

def lambda_handler(event, context):
    try:
        if event.get("httpMethod") == "OPTIONS":
            return build_response(200, {"message": "CORS preflight OK"})

        body = json.loads(event["body"])

        session_id = body["session_id"]
        project_id = body["project_id"]
        document_type = body["document_type"]
        document_text_base64 = body["document_text"]

        # 1️⃣ Fetch user info using session_id
        user = users_table.scan(
            FilterExpression="session_id = :sid",
            ExpressionAttributeValues={":sid": session_id}
        )["Items"]

        if not user:
            return build_response(404, {"error": "User not found"})

        user = user[0]
        email = user.get("email")
        user_id = user.get("id")

        # 2️⃣ Fetch project info using project_id
        project_resp = projects_table.query(
            KeyConditionExpression=Key("id").eq(project_id)
        )

        if not project_resp["Items"]:
            return build_response(404, {"error": "Project not found"})

        project_item = project_resp["Items"][0]
        project_name = project_item.get("project_name")
        organization_id = project_item.get("organization_id")

        # 3️⃣ Fetch organization info using organization_id
        org_resp = org_table.query(
            KeyConditionExpression=Key("id").eq(organization_id)
        )

        if not org_resp["Items"]:
            return build_response(404, {"error": "Organization not found"})

        org_item = org_resp["Items"][0]
        organization_name = org_item.get("organization_name")

        # ✅ 4️⃣ Check if this project_id + document_type already has a pending record
        existing_docs = review_table.query(
            KeyConditionExpression=Key("project_id").eq(project_id)
        )["Items"]

        for doc in existing_docs:
            if doc.get("document_type") == document_type and doc.get("status") == "pending":
                return build_response(400, {
                    "message": f"Document of type '{document_type}' is already in queue for review."
                })

        # 5️⃣ Generate UUID for document
        document_uuid = str(uuid.uuid4())
        document_type_uuid = f"{document_type}#{document_uuid}"

        # 6️⃣ Decode base64 and upload to S3
        file_bytes = base64.b64decode(document_text_base64)
        s3_key = f"{FOLDER_NAME}/{project_id}/{document_type_uuid}.docx"

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_bytes,
            ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        )

        s3_url = f"s3://{BUCKET_NAME}/{s3_key}"

        # 7️⃣ Save metadata into ReviewDocument table
        now = datetime.datetime.utcnow().isoformat()

        review_table.put_item(Item={
            "project_id": project_id,
            "document_type_uuid": document_type_uuid,
            "user_id": user_id,
            "email": email,
            "organization_name": organization_name,
            "project_name": project_name,
            "document_type": document_type,
            "s3_url": s3_url,
            "createdAt": now,
            "status": "pending"
        })

        return build_response(200, {
            "message": "Document uploaded successfully",
            "document_type_uuid": document_type_uuid,
            "s3_url": s3_url
        })

    except Exception as e:
        return build_response(500, {"error": str(e)})
