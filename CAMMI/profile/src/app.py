import json
import boto3
import base64
import re
import os

# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
BUCKET_NAME = "cammi-devprod"  # Your actual S3 bucket name

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(USERS_TABLE)
s3 = boto3.client("s3")

# ---------- Helper ----------
def response(status, body):
    """Return a JSON response with proper CORS headers."""
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body),
    }

# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        name = body.get("name")
        picture = body.get("picture")

        # Only session_id is mandatory
        if not session_id:
            return response(400, {"message": "session_id is required."})

        # Step 1: Find user by session_id (GSI 'session_id-index' required)
        dynamo = boto3.client("dynamodb")
        query_result = dynamo.query(
            TableName=USERS_TABLE,
            IndexName="session_id-index",
            KeyConditionExpression="session_id = :sid",
            ExpressionAttributeValues={":sid": {"S": session_id}},
        )
        print("DEBUG Query Result:", json.dumps(query_result, indent=2))
        print("DEBUG REGION:", os.environ.get("AWS_REGION"))
        print("DEBUG TABLE:", USERS_TABLE)
        print("DEBUG SESSION_ID:", session_id)
        if not query_result.get("Items"):
            return response(404, {"message": "User not found."})

        user = query_result["Items"][0]
        email = user["email"]["S"]
        user_id = user["id"]["S"]

        update_expression_parts = []
        expression_values = {}
        expr_names = {}
        picture_url = None

        # ---------- Step 2: If picture provided, upload to S3 ----------
        if picture:
            match = re.match(r"data:image/(\w+);base64,", picture)
            if not match:
                return response(400, {"message": "Invalid picture format (must be base64)."})
            file_type = match.group(1)
            base64_data = re.sub(r"^data:image/\w+;base64,", "", picture)
            image_bytes = base64.b64decode(base64_data)
            s3_key = f"profile/{user_id}.{file_type}"

            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=image_bytes,
                ContentType=f"image/{file_type}",
            )

            picture_url = f"https://{BUCKET_NAME}.s3.amazonaws.com/{s3_key}"
            update_expression_parts.append("picture = :pic")
            expression_values[":pic"] = picture_url

        # ---------- Step 3: If name provided, update accordingly ----------
        if name:
            # split once: first token = firstName, rest = lastName
            parts = name.strip().split(" ", 1)
            first_name = parts[0]
            last_name = parts[1] if len(parts) > 1 else ""

            if "name" in user:
                # Google Sign-In user
                # SYNC for login/logout: also update firstName & lastName so UI (first+last) reflects changes
                update_expression_parts.append("#nm = :nm")
                expression_values[":nm"] = name
                expr_names["#nm"] = "name"

                update_expression_parts.append("firstName = :fname")
                update_expression_parts.append("lastName  = :lname")
                expression_values[":fname"] = first_name
                expression_values[":lname"] = last_name
            else:
                # Email Sign-Up user
                # Keep original behavior AND (optional) keep 'name' in sync for consistency across the app
                update_expression_parts.append("firstName = :fname")
                update_expression_parts.append("lastName  = :lname")
                expression_values[":fname"] = first_name
                expression_values[":lname"] = last_name

                update_expression_parts.append("#nm = :nm")
                expression_values[":nm"] = name
                expr_names["#nm"] = "name"

        # ---------- Step 4: If nothing to update ----------
        if not update_expression_parts:
            return response(400, {"message": "No fields provided to update."})

        # ---------- Step 5: Update DynamoDB ----------
        update_expression = "SET " + ", ".join(update_expression_parts)

        update_kwargs = {
            "Key": {"email": email},
            "UpdateExpression": update_expression,
            "ExpressionAttributeValues": expression_values,
            "ReturnValues": "UPDATED_NEW",
        }
        if expr_names:
            update_kwargs["ExpressionAttributeNames"] = expr_names

        table.update_item(**update_kwargs)

        return response(200, {
            "message": "User profile updated successfully.",
            "picture_url": picture_url,
        })

    except Exception as e:
        print("Error:", e)
        return response(500, {"message": f"Internal Server Error: {str(e)}"})
