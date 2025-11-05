import json
import uuid
from datetime import datetime
import boto3
from boto3.dynamodb.conditions import Attr

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')

# Tables
organizations_table = dynamodb.Table('projects-table')
users_table = dynamodb.Table('users-table')
project_questions_table = dynamodb.Table('project-questions-table')

# CORS headers
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, project_id, session_id"
}


def lambda_handler(event, context):
    # Handle preflight OPTIONS request safely
    if event.get('httpMethod') == 'OPTIONS':
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight"})
        }

    # Extract headers
    project_id = event['headers'].get('project_id')
    session_id = event['headers'].get('session_id')

    if not project_id or not session_id:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Missing project_id or session_id in headers"})
        }

    # Extract body
    try:
        body = json.loads(event['body'])
        question_text = body.get('question_text')
        answer_text = body.get('answer_text')
        document_type = body.get('document_type')
    except Exception as e:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": f"Invalid JSON body: {str(e)}"})
        }

    if not question_text or answer_text is None:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Missing question_text or answer_text in body"})
        }

    # Validate project existence
    project_response = organizations_table.scan(
        FilterExpression=Attr('id').eq(project_id)
    )
    if not project_response['Items']:
        return {
            "statusCode": 404,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Project not found"})
        }

    # Validate user session
    user_response = users_table.scan(
        FilterExpression=Attr('session_id').eq(session_id)
    )
    if not user_response['Items']:
        return {
            "statusCode": 401,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Invalid session"})
        }

    user_id = user_response['Items'][0]['id']
    timestamp = datetime.utcnow().isoformat()

    # --- 🔍 Check if question already exists for this user and project ---
    existing_question = project_questions_table.scan(
        FilterExpression=Attr('project_id').eq(project_id)
                        & Attr('user_id').eq(user_id)
                        & Attr('question_text').eq(question_text)
    )

    if existing_question['Items']:
        # ✅ Update existing question
        item = existing_question['Items'][0]
        question_id = item['question_id']

        project_questions_table.update_item(
            Key={
                'project_id': project_id,
                'question_id': question_id
            },
            UpdateExpression="""
                SET answer_text = :answer,
                    updatedAt = :updated,
                    #st = :status
            """,
            ExpressionAttributeValues={
                ':answer': answer_text,
                ':updated': timestamp,
                ':status': 'answered' if answer_text else 'pending'
            },
            ExpressionAttributeNames={
                '#st': 'status'
            }
        )

        message = "Existing question updated successfully"
    else:
        # 🆕 Insert new question
        question_id = str(uuid.uuid4())
        project_questions_table.put_item(
            Item={
                'project_id': project_id,
                'question_id': question_id,
                'question_text': question_text,
                'answer_text': answer_text,
                'user_id': user_id,
                'createdAt': timestamp,
                'updatedAt': timestamp,
                'status': 'answered' if answer_text else 'pending'
            }
        )
        message = "New question inserted successfully"

    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({
            "message": message,
            "question_id": question_id,
            "document_type": document_type
        })
    }
