import json
import boto3
import base64

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
TABLE_NAME = "review-documents-table"
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    # Handle CORS preflight request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "POST,OPTIONS",
                "Access-Control-Allow-Headers": "Content-Type,Authorization",
                "Access-Control-Max-Age": "3600"
            },
            "body": ""
        }

    try:
        # Parse input JSON
        body = json.loads(event.get("body", "{}"))
        project_id = body.get("project_id")
        document_type_uuid = body.get("document_type_uuid")

        if not project_id or not document_type_uuid:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "project_id and document_type_uuid are required"})
            }

        # Fetch the document record from DynamoDB
        response = table.get_item(
            Key={
                "project_id": project_id,
                "document_type_uuid": document_type_uuid
            }
        )

        item = response.get("Item")
        if not item:
            return {
                "statusCode": 404,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Document not found"})
            }

        # Extract S3 URL and parse bucket/key
        s3_url = item.get("s3_url")
        if not s3_url:
            return {
                "statusCode": 500,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "S3 URL not found in DynamoDB"})
            }

        bucket = s3_url.split("/")[2]
        key = "/".join(s3_url.split("/")[3:])

        # Fetch the file from S3
        s3_response = s3.get_object(Bucket=bucket, Key=key)
        file_content = s3_response["Body"].read()

        # Encode as Base64
        encoded_content = base64.b64encode(file_content).decode("utf-8")
        filename = f"{item.get('document_type', 'document')}-{item.get('email', 'user')}.docx"

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "filename": filename,
                "docxBase64": encoded_content
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"error": str(e)})
        }
