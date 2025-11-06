import json
import boto3
from botocore.exceptions import ClientError

# DynamoDB table connection
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("linkedin-posts-table")


def delete_post(sub: str, post_time: str):
    """Delete an item only if status == 'pending'."""
    key = {"sub": sub, "post_time": post_time}

    try:
        response = table.delete_item(
            Key=key,
            ConditionExpression="#s = :pending",
            ExpressionAttributeValues={":pending": "pending"},
            ExpressionAttributeNames={"#s": "status"},  # handle reserved word
        )
        return {"success": True, "response": response}

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"success": False, "error": "Delete allowed only if status = pending"}
        return {"success": False, "error": str(e)}


def edit_post(sub: str, post_time: str, new_values: dict):
    """Update an item only if status == 'pending'."""
    key = {"sub": sub, "post_time": post_time}

    # Fields that can be updated
    editable_fields = ["image_keys", "message", "post_urn", "scheduled_time", "status", "status_code"]

    update_expr = []
    expr_attr_vals = {":pending": "pending"}  # condition check
    expr_attr_names = {"#s": "status"}  # reserved keyword

    for field, value in new_values.items():
        if field not in editable_fields:
            return {"success": False, "error": f"Field '{field}' is not editable"}

        placeholder = f"#attr_{field}"
        value_placeholder = f":new_{field}"

        update_expr.append(f"{placeholder} = {value_placeholder}")
        expr_attr_vals[value_placeholder] = value
        expr_attr_names[placeholder] = field

    if not update_expr:
        return {"success": False, "error": "No valid fields to update"}

    update_expr_str = "SET " + ", ".join(update_expr)

    try:
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expr_str,
            ConditionExpression="#s = :pending",
            ExpressionAttributeValues=expr_attr_vals,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW",
        )
        return {"success": True, "response": response}

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return {"success": False, "error": "Edit allowed only if status = pending"}
        return {"success": False, "error": str(e)}


def build_response(status_code, body_dict):
    """Standard API Gateway Lambda proxy response with CORS."""
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT,DELETE",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(body_dict),
    }


def lambda_handler(event, context):
    """Main Lambda function."""
    try:
        # Parse JSON body from event
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")
        sub = body.get("sub")
        post_time = body.get("post_time")

        # Validate required fields
        if not action or not sub or not post_time:
            return build_response(400, {
                "error": "Missing required fields: 'action', 'sub', or 'post_time'"
            })

        # Handle delete action
        if action == "delete":
            result = delete_post(sub, post_time)
            return build_response(200, result)

        # Handle edit action
        elif action == "edit":
            new_values = body.get("new_values", {})
            if not isinstance(new_values, dict):
                return build_response(400, {"error": "'new_values' must be a JSON object"})
            result = edit_post(sub, post_time, new_values)
            return build_response(200, result)

        # Unsupported action
        else:
            return build_response(400, {
                "error": "Invalid action. Only 'delete' or 'edit' are allowed."
            })

    except Exception as e:
        return build_response(500, {"error": f"Internal server error: {str(e)}"})
