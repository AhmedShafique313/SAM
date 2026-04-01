import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key
 
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
 
users_table = dynamodb.Table("users-table")
 
# Fixed S3 bucket name
BUCKET_NAME = "cammi-devprod"
 
 
# ---------------- USER HELPERS ----------------
def get_user_by_session(session_id):
    res = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )
    return res["Items"][0] if res.get("Items") else None
 
 
def update_user_credits(email, amount):
    users_table.update_item(
        Key={"email": email},
        UpdateExpression="SET total_credits = :v",
        ExpressionAttributeValues={":v": amount},
    )
 
 
# ---------------- MAIN ----------------
def lambda_handler(event, context):
 
    body = json.loads(event.get("body", "{}"))
 
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    file_name = body.get("file_name")
 
    if not session_id or not project_id:
        return _response(
            400,
            {"error": "Missing session_id and project_id in request body."}
        )
 
    # ================= USER FETCH =================
    user = get_user_by_session(session_id)
 
    if not user:
        return _response(404, {"error": "User not found"})
 
    email = user["email"]
 
    # ================= CREDIT CHECK =================
    credits = int(user.get("total_credits", 0))
 
    if credits < 2:
        return _response(
            402,
            {"error": "Insufficient credits"}
        )
 
    # Deduct 2 credits
    new_credits = max(credits - 2, 0)
    update_user_credits(email, new_credits)
 
    # ================= FILE NAME =================
    if not file_name:
        file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.pdf"
 
    # ================= S3 PATH =================
    object_key = f"pdf_files/{project_id}/{session_id}/{file_name}"
 
    # ================= PRESIGNED URL =================
    presigned_url = s3.generate_presigned_url(
        ClientMethod="put_object",
        Params={
            "Bucket": BUCKET_NAME,
            "Key": object_key,
            "ContentType": "application/pdf",
            "Metadata": {
                "session-id": session_id,
                "project-id": project_id,
                "filename": file_name
            }
        },
        ExpiresIn=300,
        HttpMethod="PUT"
    )
 
    response_body = {
        "upload_url": presigned_url,
        "s3_path": f"s3://{BUCKET_NAME}/{object_key}",
        "file_name": file_name,
        "session_id": session_id,
        "project_id": project_id,
        "remaining_credits": new_credits
    }
 
    return _response(200, response_body)
 
 
# ---------------- RESPONSE ----------------
def _response(status_code, body_obj):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body_obj)
    }