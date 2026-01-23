import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("posts-table")


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    # Required fields
    post_id = body.get("post_id")
    campaign_id = body.get("campaign_id")

    if not post_id or not campaign_id:
        return build_response(400, {"error": "post_id and campaign_id are required"})

    # Optional editable fields
    optional_fields = {
        "title": body.get("title"),
        "description": body.get("description"),
        "image_generation_prompt": body.get("image_generation_prompt"),
        "best_post_day": body.get("best_post_day"),
        "best_post_time": body.get("best_post_time"),
    }

    # Check if post exists
    post_resp = posts_table.get_item(
        Key={
            "post_id": post_id,
            "campaign_id": campaign_id
        }
    )

    if "Item" not in post_resp:
        return build_response(404, {"error": "Post not found"})

    # Build dynamic update expression
    update_expressions = []
    expression_attribute_names = {}
    expression_attribute_values = {}

    for field, value in optional_fields.items():
        if value is not None:
            update_expressions.append(f"#{field} = :{field}")
            expression_attribute_names[f"#{field}"] = field
            expression_attribute_values[f":{field}"] = value

    # Always update status + updated_at
    update_expressions.append("#status = :status")
    update_expressions.append("#updated_at = :updated_at")

    expression_attribute_names["#status"] = "status"
    expression_attribute_names["#updated_at"] = "updated_at"

    expression_attribute_values[":status"] = "Edited"
    expression_attribute_values[":updated_at"] = datetime.utcnow().isoformat()

    posts_table.update_item(
        Key={
            "post_id": post_id,
            "campaign_id": campaign_id
        },
        UpdateExpression="SET " + ", ".join(update_expressions),
        ExpressionAttributeNames=expression_attribute_names,
        ExpressionAttributeValues=expression_attribute_values,
        ReturnValues="ALL_NEW"
    )

    return build_response(200, {
        "message": "Post updated successfully",
        "updated_fields": [k for k, v in optional_fields.items() if v is not None]
    })


def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }
