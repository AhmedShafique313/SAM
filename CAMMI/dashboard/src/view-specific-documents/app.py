import json
import boto3
import base64
from urllib.parse import urlparse
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

DOCUMENT_HISTORY_TABLE = "documents-history-table"

def lambda_handler(event, context):
    try:
        # ✅ Parse input
        body = json.loads(event.get("body", "{}"))
        user_id = body.get("user_id")
        document_type_uuid = body.get("document_type_uuid")

        if not user_id or not document_type_uuid:
            return _error_response("Missing required fields: user_id or document_type_uuid")

        # ✅ Step 1: Query DocumentHistory by user_id (partition key) + document_type_uuid (sort key)
        doc_table = dynamodb.Table(DOCUMENT_HISTORY_TABLE)
        response = doc_table.query(
            KeyConditionExpression=Key("user_id").eq(user_id) & Key("document_type_uuid").eq(document_type_uuid)
        )

        items = response.get("Items", [])
        if not items:
            return _error_response(f"No record found for user_id={user_id}, document_type_uuid={document_type_uuid}")

        document_item = items[0]
        document_url = document_item.get("document_url")

        if not document_url:
            return _error_response("Document record does not contain 'document_url'")

        # ✅ Step 2: Parse the S3 URL
        bucket_name, object_key = _parse_s3_url(document_url)
        if not bucket_name or not object_key:
            return _error_response("Invalid S3 URL format")

        # ✅ Step 3: Download the file from S3
        s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
        file_content = s3_object["Body"].read()

        # ✅ Step 4: Convert file to base64
        base64_encoded = base64.b64encode(file_content).decode("utf-8")

        # ✅ Step 5: Return response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "user_id": user_id,
                "document_type_uuid": document_type_uuid,
                "document_url": document_url,
                "document_base64": base64_encoded
            })
        }

    except Exception as e:
        return _error_response(str(e))


def _parse_s3_url(s3_url):
    """
    Supports both s3://bucket/key and https://bucket.s3.amazonaws.com/key formats.
    """
    parsed = urlparse(s3_url)

    if parsed.scheme == "s3":
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
    elif parsed.scheme in ["http", "https"] and ".s3" in parsed.netloc:
        # Handles https://bucket.s3.amazonaws.com/key
        bucket = parsed.netloc.split(".s3")[0]
        key = parsed.path.lstrip("/")
    else:
        bucket = None
        key = None

    return bucket, key


def _error_response(message, code=400):
    return {
        "statusCode": code,
        "body": json.dumps({"error": message})
    }
