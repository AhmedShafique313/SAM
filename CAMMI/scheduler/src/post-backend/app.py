import json
import uuid
import boto3
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr

dynamodb = boto3.resource('dynamodb')
post_questions_table = dynamodb.Table('post-questions-table')
organization_table = dynamodb.Table('organizations-table')

# The specific question to check
TARGET_QUESTION = "What proof points or key statistics do you want highlighted?"

def lambda_handler(event, context):
    try:
        # ✅ CORS preflight request
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                    "Access-Control-Allow-Headers": "Content-Type,Authorization"
                },
                "body": json.dumps({"message": "CORS preflight success"})
            }

        # ✅ Check HTTP method
        if event.get("httpMethod") != "POST":
            return {
                "statusCode": 405,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Method not allowed"})
            }

        # ✅ Parse JSON body
        body = json.loads(event.get("body", "{}"))
        organization_id = body.get("organization_id")
        question = body.get("post_question")
        answer = body.get("post_answer")

        # ✅ Validate required fields
        if not organization_id or not question or not answer:
            return {
                "statusCode": 400,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({"error": "Missing required fields"})
            }

        # ✅ Step 1: Check if question already exists for this organization
        existing_record = post_questions_table.scan(
            FilterExpression=Attr("organization_id").eq(organization_id) &
                             Attr("post_question").eq(question)
        )

        if existing_record.get("Items"):
            # ✅ Step 2: Update existing record
            existing_id = existing_record["Items"][0]["id"]
            post_questions_table.update_item(
                Key={"id": existing_id},
                UpdateExpression="SET post_answer = :ans, updated_at = :time",
                ExpressionAttributeValues={
                    ":ans": answer,
                    ":time": datetime.now(timezone.utc).isoformat()
                }
            )
            action = "updated"
            record_id = existing_id
        else:
            # ✅ Step 3: Insert new record
            record_id = str(uuid.uuid4())
            record = {
                "id": record_id,
                "organization_id": organization_id,
                "post_question": question,
                "post_answer": answer,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            post_questions_table.put_item(Item=record)
            action = "inserted"

        # ✅ Step 4: Check if question matches the target question → update Organizations table
        if question.strip().lower() == TARGET_QUESTION.lower():
            organization_table.update_item(
                Key={"id": organization_id},
                UpdateExpression="SET post_question_flag = :val",
                ExpressionAttributeValues={":val": False}
            )

        # ✅ Step 5: Return success response
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization"
            },
            "body": json.dumps({
                "message": f"Record {action} successfully",
                "record_id": record_id
            })
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization"
            },
            "body": json.dumps({"error": str(e)})
        }
