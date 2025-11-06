import json
import boto3
import base64
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

# DynamoDB table name
TABLE_NAME = "linkedin-posts-table"

# S3 bucket name
BUCKET_NAME = "cammi-devprod"

def lambda_handler(event, context):
    # Enable CORS headers
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
    }

    try:
        # Parse request body
        body = json.loads(event.get("body", "{}"))
        sub = body.get("sub")
        post_time = body.get("post_time")

        if not sub or not post_time:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Missing sub or post_time"})
            }

        # Fetch row from DynamoDB
        table = dynamodb.Table(TABLE_NAME)
        response = table.get_item(
            Key={"sub": sub, "post_time": post_time}
        )

        item = response.get("Item")
        if not item:
            return {
                "statusCode": 404,
                "headers": headers,
                "body": json.dumps({"error": "Item not found"})
            }

        # Process image_urls: fetch from S3 and convert to Base64
        images_base64 = []
        for key in item.get("image_keys", []):
            try:
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                img_bytes = obj["Body"].read()
                img_b64 = base64.b64encode(img_bytes).decode("utf-8")
                images_base64.append(img_b64)
            except ClientError as e:
                images_base64.append(f"Error fetching {key}: {str(e)}")

        # Replace image_urls with base64 data
        item["image_keys"] = images_base64

        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps(item)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": headers,
            "body": json.dumps({"error": str(e)})
        }
