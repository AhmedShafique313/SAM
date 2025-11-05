import json
import boto3
from boto3.dynamodb.conditions import Key, Attr
 
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("project-questions-table")
 
def lambda_handler(event, context):
    # Enable CORS
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,PUT",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }
   
    try:
        # Extract project_id from headers
        project_id = event.get("headers", {}).get("project_id")
        if not project_id:
            return {"statusCode": 400, "headers": headers, "body": json.dumps({"error": "Missing project_id in headers"})}
 
        # Extract question_text and answer_text from body
        body = json.loads(event.get("body", "{}"))
        question_text = body.get("question_text")
        answer_text = body.get("answer_text")
 
        if not question_text or not answer_text:
            return {"statusCode": 400, "headers": headers, "body": json.dumps({"error": "Missing question_text or answer_text"})}
 
        # Query items by project_id
        response = table.query(
            KeyConditionExpression=Key("project_id").eq(project_id)
        )
 
        items = response.get("Items", [])
        # Find the item with matching question_text
        target_item = next((item for item in items if item.get("question_text") == question_text), None)
 
        if not target_item:
            return {"statusCode": 404, "headers": headers, "body": json.dumps({"error": "Question not found"})}
 
        # Update only the answer_text using PK + SK
        table.update_item(
            Key={
                "project_id": project_id,
                "question_id": target_item["question_id"]  # Use the sort key
            },
            UpdateExpression="SET answer_text = :a",
            ExpressionAttributeValues={":a": answer_text}
        )
 
        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps({"message": "Answer updated successfully"})
        }
 
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": headers,
            "body": json.dumps({"error": str(e)})
        }