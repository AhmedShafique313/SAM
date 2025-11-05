import json
import base64
import boto3
import os
from boto3.dynamodb.conditions import Attr

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

BUCKET_NAME = "cammi-devprod"
USERS_TABLE = os.environ.get('USERS_TABLE', 'users-table')
users_table = dynamodb.Table(USERS_TABLE)

ALLOWED_ORIGIN = "*"

# Document credit cost mapping
DOCUMENT_CREDITS = {
    "gtm": 25,  # GTM Document
    "icp": 3,   # ICP Document
    "kmf": 3,   # Key Messaging
    "sr": 4,    # Strategy Roadmap
    "bs": 3     # Brand Strategy
}

def response(status_code, message):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
            "Content-Type": "application/json"
        },
        "body": json.dumps({"message": message})
    }

def lambda_handler(event, context):
    try:
        # --- Handle CORS ---
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
                    "Access-Control-Allow-Headers": "Content-Type,Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,POST"
                },
                "body": ""
            }

        if not event.get("body"):
            return response(400, "Empty body received")

        # --- Parse body ---
        body = json.loads(event["body"])
        file_name = body["fileName"].replace('.txt', '').replace(' ', '_')
        raw_content = base64.b64decode(body["fileContent"]).decode("utf-8")
        project_id = body.get("project_id", "")
        session_id = body.get("token", "")
        document_type = body.get("document_type", "")

        if not session_id:
            return response(400, "session_id is required")

        # --- Query user by session_id ---
        table = dynamodb.Table(USERS_TABLE)
        response_scan = table.scan(
            FilterExpression=Attr('session_id').eq(session_id)
        )

        items = response_scan.get('Items', [])
        if not items:
            return response(404, "User not found for the given session_id")

        # --- Extract user data ---
        user_item = items[0]
        total_credits = user_item.get('total_credits', 0)

        # --- Credit check ---
        doc_credits = DOCUMENT_CREDITS.get(document_type)
        if doc_credits is None:
            return response(400, f"Invalid document type: {document_type}")

        if total_credits < doc_credits:
            # ❌ Stop immediately if not enough credits
            return response(400, f"Insufficient credits. Required: {doc_credits}, Available: {total_credits}")

        # ✅ Enough credits → deduct and update
        new_credits = total_credits - doc_credits
        users_table.update_item(
            Key={'email': user_item['email']},  # <-- partition key is email
            UpdateExpression="SET total_credits = :new_credits",
            ExpressionAttributeValues={':new_credits': new_credits}
        )

        # --- Continue normal flow ---
        try:
            parsed_json = json.loads(raw_content)
            formatted_text = ""
            for item in parsed_json:
                q = item.get("question_text", "").strip()
                a = item.get("answer_text", "").strip()
                if q or a:
                    formatted_text += f"Q: {q}\nA: {a}\n\n"
        except Exception:
            formatted_text = raw_content

        # --- Upload file to S3 ---
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"{document_type}/{project_id}/prompt/businessidea/businessidea/{file_name}.txt",
            Body=formatted_text.encode("utf-8"),
            ContentType="text/plain",
            Metadata={
                "token": session_id,
                "project_id": project_id,
                "document_type": document_type
            }
        )

        # --- Return success message ---
        used_msg = (
            f"{file_name}.txt uploaded successfully. "
            f"{doc_credits} credits used for {document_type} document. "
            f"Remaining credits: {new_credits}"
        )
        return response(200, used_msg)

    except Exception as e:
        return response(500, f"Error: {str(e)}")
