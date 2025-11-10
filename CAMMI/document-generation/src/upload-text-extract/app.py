# websocket_handler.py
import boto3
import json
import uuid
import hashlib
import re
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Attr
import os
import traceback
from boto3.dynamodb.conditions import Key

# ---------- AWS Clients ----------
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
PROJECT_QUESTIONS_TABLE = os.environ.get("PROJECT_QUESTIONS_TABLE", "project-questions-table")

# ---------- Connection Management ----------
def store_connection_in_user(session_id, connection_id):
    """Store WebSocket connection_id in Users table"""
    table = dynamodb.Table(USERS_TABLE)
    
    # First find the user by session_id
    resp = table.scan(
        FilterExpression=Attr("session_id").eq(session_id),
        ProjectionExpression="email"
    )
    items = resp.get("Items", [])
    if not items:
        raise ValueError(f"Session_id {session_id} not found in Users table")
    
    user_email = items[0]["email"]
    
    # Update the user record with connection_id
    table.update_item(
        Key={"email": user_email},
        UpdateExpression="SET connection_id = :c, connection_updated_at = :t",
        ExpressionAttributeValues={
            ":c": connection_id,
            ":t": datetime.utcnow().isoformat()
        }
    )
    
    return user_email

def remove_connection_from_user(connection_id):
    """Remove WebSocket connection_id from Users table"""
    table = dynamodb.Table(USERS_TABLE)
    
    # Find user by connection_id
    try:
        resp = table.scan(
            FilterExpression=Attr("connection_id").eq(connection_id),
            ProjectionExpression="email"
        )
        items = resp.get("Items", [])
        if items:
            user_email = items[0]["email"]
            # Remove connection_id from user
            table.update_item(
                Key={"email": user_email},
                UpdateExpression="REMOVE connection_id, connection_updated_at"
            )
    except Exception as e:
        print(f"Error removing connection: {e}")

def get_user_by_session(session_id):
    """Get user info by session_id"""
    table = dynamodb.Table(USERS_TABLE)
    resp = table.scan(
        FilterExpression=Attr("session_id").eq(session_id),
        ProjectionExpression="email, id"
    )
    items = resp.get("Items", [])
    if not items:
        raise ValueError(f"Session_id {session_id} not found in Users table")
    return items[0]

# ---------- API Gateway Management ----------
def get_apigw_client(event):
    """Get API Gateway Management API client"""
    domain = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    endpoint = f"https://{domain}/{stage}"
    return boto3.client("apigatewaymanagementapi", endpoint_url=endpoint)

def send_to_client(apigw_client, connection_id, data):
    """Send data to WebSocket client"""
    try:
        apigw_client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(data).encode("utf-8")
        )
        return True
    except ClientError as e:
        status_code = int(e.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 500))
        if status_code == 410:  # Connection closed
            remove_connection_from_user(connection_id)
        return False
    except Exception as e:
        print(f"Error sending to client: {e}")
        return False

# ---------- Bedrock Processing ----------
def invoke_bedrock_icp(text: str, session_id: str) -> str:
    """Call Bedrock for processing"""
    instruction = """
You are an expert business analyst.

You will be given a single document. Your task is to carefully read the entire text and extract answers to the following questions:

1) What is your business called?
2) How would you describe what you do in a few sentences?
3) Who is your main customer right now?
4) What are the top 3 goals you want to hit in the next 12 months?
5) What challenges are you facing in finding or converting customers?
6) Do you already know your best-fit customers or industries?
7) Who are your main competitors?

STRICT INSTRUCTIONS:
- Return ONLY a flat JSON object where each key is exactly the question string above and the value is the best possible answer.
- Do NOT include extra fields, explanations, evidence, or commentary. JSON only.
- Answers must come directly from the document. If the document phrases things differently (e.g., "objectives," "targets," "priorities" instead of "goals"), treat them as valid answers.
- Rephrase and summarize where needed to produce a clear and complete answer.
- If an answer is mentioned in multiple ways across the document, combine into a single concise response.
- If a question cannot be answered from the document in any explicit or implicit form, output "Not Found".
- Never hallucinate facts that are not grounded in the document.
- Your answer must conatin strict JSON because my next flow is expecting JSON.

OUTPUT FORMAT (strict):
{
  "What is your business called?": "...",
  "How would you describe what you do in a few sentences?": "...",
  "Who is your main customer right now?": "...",
  "What are the top 3 goals you want to hit in the next 12 months?": "...",
  "What challenges are you facing in finding or converting customers?": "...",
  "Do you already know your best-fit customers or industries?": "...",
  "Who are your main competitors?": "..."
}

"""
    conversation = [
        {"role": "user", "content": [{"text": instruction}]},
        {"role": "user", "content": [{"text": text.strip()}]}
    ]

    response = bedrock_runtime.converse(
        modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
        messages=conversation,
        inferenceConfig={"temperature": 0.5, "topP": 0.9},
        requestMetadata={"sessionId": session_id}
    )

    return response["output"]["message"]["content"][0]["text"]

def invoke_bedrock_kmf(text: str, session_id: str) -> str:
    """Call Bedrock for processing"""
    instruction = """
You are an expert business analyst.

You will be given a single document. Your task is to carefully read the entire text and extract answers to the following questions:

1) What is the name of your business?
2) Which industry does your business belong to?
3) What is the main objective or goal of your business?
4) How would you describe your business concept?
5) Who is your target market or ideal customer?
6) What is your short value proposition (how you help customers in a simple way)?
7) What is the long-term vision for your business?
8) What key problems does your business solve for customers?
9) What are the core products or services your business offers?
10) What makes your business unique compared to competitors?
11) What tone or personality should your brand convey (e.g., professional, friendly, innovative)?
12) Are there any additional themes or values you want associated with your brand?

STRICT INSTRUCTIONS:
- Return ONLY a flat JSON object where each key is exactly the question string above and the value is the best possible answer.
- Do NOT include extra fields, explanations, or commentary. JSON only.
- Answers must come directly from the document. If the document phrases things differently (e.g., “mission,” “purpose,” “aim” instead of “objective”), treat them as valid answers.
- Rephrase and summarize where needed to produce a clear and complete answer.
- If an answer is mentioned in multiple places across the document, combine into a single concise response.
- If a question cannot be answered from the document in any explicit or implicit form, output "Not Found".
- Never hallucinate facts that are not grounded in the document.
- Your answer must conatin strict JSON because my next flow is expecting JSON.

OUTPUT FORMAT (strict):
{
  "What is the name of your business?": "...",
  "Which industry does your business belong to?": "...",
  "What is the main objective or goal of your business?": "...",
  "How would you describe your business concept?": "...",
  "Who is your target market or ideal customer?": "...",
  "What is your short value proposition (how you help customers in a simple way)?": "...",
  "What is the long-term vision for your business?": "...",
  "What key problems does your business solve for customers?": "...",
  "What are the core products or services your business offers?": "...",
  "What makes your business unique compared to competitors?": "...",
  "What tone or personality should your brand convey (e.g., professional, friendly, innovative)?": "...",
  "Are there any additional themes or values you want associated with your brand?": "..."
}

"""
    conversation = [
        {"role": "user", "content": [{"text": instruction}]},
        {"role": "user", "content": [{"text": text.strip()}]}
    ]

    response = bedrock_runtime.converse(
        modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
        messages=conversation,
        inferenceConfig={"temperature": 0.5, "topP": 0.9},
        requestMetadata={"sessionId": session_id}
    )

    return response["output"]["message"]["content"][0]["text"]

def invoke_bedrock_sr(text: str, session_id: str) -> str:
    """Call Bedrock for processing"""
    instruction = """
You are an expert business analyst.

You will be given a single document (document or text from the document). Your task is to carefully read the entire text and extract answers to the following questions:

1) What is the name of your business?
2) How would you describe your business concept in one or two sentences?
3) What is the main objective or goal of your business?
4) What is your short value proposition (how you help customers in a simple way)?
5) Which industry does your business belong to?
6) What is your business model (e.g., subscription, pay-per-download, freemium)?
7) Who is your target market or ideal customer?
8) What is your geographic market focus?
9) How do you position your pricing (e.g., low-tier, mid-tier, premium)?
10) Who are your main competitors?
11) What is your estimated marketing budget?
12) What stage of development is your business currently in?
13) What are your top user development priorities?
14) What are your key marketing objectives?
15) When do you plan to start this project?
16) When is your planned end date or long-term milestone?

STRICT INSTRUCTIONS:
- Return ONLY a flat JSON object where each key is exactly the question string above and the value is the best possible answer.
- Do NOT include extra fields, explanations, evidence, or commentary. JSON only.
- Answers must come directly from the document. If the document phrases things differently (e.g., “mission,” “purpose,” or “aim” instead of “objective”; “audience” instead of “target market”), treat them as valid answers.
- Rephrase and summarize where needed to produce a clear and complete answer.
- If an answer is mentioned in multiple places across the document, combine into a single concise response.
- If a question cannot be answered from the document in any explicit or implicit form, output "Not Found".
- Never hallucinate facts that are not grounded in the document.
- Your answer must conatin strict JSON because my next flow is expecting JSON.

OUTPUT FORMAT (strict):
{
  "What is the name of your business?": "...",
  "How would you describe your business concept in one or two sentences?": "...",
  "What is the main objective or goal of your business?": "...",
  "What is your short value proposition (how you help customers in a simple way)?": "...",
  "Which industry does your business belong to?": "...",
  "What is your business model (e.g., subscription, pay-per-download, freemium)?": "...",
  "Who is your target market or ideal customer?": "...",
  "What is your geographic market focus?": "...",
  "How do you position your pricing (e.g., low-tier, mid-tier, premium)?": "...",
  "Who are your main competitors?": "...",
  "What is your estimated marketing budget?": "...",
  "What stage of development is your business currently in?": "...",
  "What are your top user development priorities?": "...",
  "What are your key marketing objectives?": "...",
  "When do you plan to start this project?": "...",
  "When is your planned end date or long-term milestone?": "..."
}

"""
    conversation = [
        {"role": "user", "content": [{"text": instruction}]},
        {"role": "user", "content": [{"text": text.strip()}]}
    ]

    response = bedrock_runtime.converse(
        modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
        messages=conversation,
        inferenceConfig={"temperature": 0.5, "topP": 0.9},
        requestMetadata={"sessionId": session_id}
    )

    return response["output"]["message"]["content"][0]["text"]

def invoke_bedrock_bs(text: str, session_id: str) -> str:
    """Call Bedrock for processing"""
    instruction = """
You are an expert business analyst.

You will be given a single document. Your task is to carefully read the entire text and extract answers to the following questions:

1) What is the name of your business?
2) How would you describe your business concept?
3) What is your value proposition (the main benefit your platform offers customers)?
4) Which industry does your business belong to?
5) Who is your target market or ideal customer?
6) What is your primary geographic market focus?
7) How do you position your pricing (e.g., low-tier, mid-tier, premium)?
8) Who are your main competitors?
9) When do you plan to start this project?
10) What is your long-term end date or milestone?
11) Which customers are approved to be publicly showcased?
12) Can you provide links to approved customer video assets?
13) Can you provide links to approved customer case studies?
14) What are the approved customer quotes you want to feature?
15) Can you provide links to approved customer logos or other visual assets?
16) What brag points or achievements would you like to highlight?
17) Who will act as the spokesperson for your business?
18) What is the spokesperson’s title or role?
19) Can you provide links to your brand’s visual assets (e.g., logo, product screenshots)?

STRICT INSTRUCTIONS:
- Return ONLY a flat JSON object where each key is exactly the question string above and the value is the best possible answer.
- Do NOT include extra fields, explanations, or commentary. JSON only.
- Answers must come directly from the document. If the document phrases things differently (e.g., “advantage” instead of “value proposition,” “reference customer” instead of “approved showcase”), treat them as valid answers.
- Rephrase and summarize where needed to produce a clear and complete answer.
- If an answer is mentioned in multiple places across the document, combine into a single concise response.
- If a question cannot be answered from the document in any explicit or implicit form, output "Not Found".
- Never hallucinate facts that are not grounded in the document.
- Your answer must conatin strict JSON because my next flow is expecting JSON.

OUTPUT FORMAT (strict):
{
  "What is the name of your business?": "...",
  "How would you describe your business concept?": "...",
  "What is your value proposition (the main benefit your platform offers customers)?": "...",
  "Which industry does your business belong to?": "...",
  "Who is your target market or ideal customer?": "...",
  "What is your primary geographic market focus?": "...",
  "How do you position your pricing (e.g., low-tier, mid-tier, premium)?": "...",
  "Who are your main competitors?": "...",
  "When do you plan to start this project?": "...",
  "What is your long-term end date or milestone?": "...",
  "Which customers are approved to be publicly showcased?": "...",
  "Can you provide links to approved customer video assets?": "...",
  "Can you provide links to approved customer case studies?": "...",
  "What are the approved customer quotes you want to feature?": "...",
  "Can you provide links to approved customer logos or other visual assets?": "...",
  "What brag points or achievements would you like to highlight?": "...",
  "Who will act as the spokesperson for your business?": "...",
  "What is the spokesperson’s title or role?": "...",
  "Can you provide links to your brand’s visual assets (e.g., logo, product screenshots)?": "..."
}

"""
    conversation = [
        {"role": "user", "content": [{"text": instruction}]},
        {"role": "user", "content": [{"text": text.strip()}]}
    ]

    response = bedrock_runtime.converse(
        modelId="us.anthropic.claude-sonnet-4-20250514-v1:0",
        messages=conversation,
        inferenceConfig={"temperature": 0.5, "topP": 0.9},
        requestMetadata={"sessionId": session_id}
    )

    return response["output"]["message"]["content"][0]["text"]


def extract_json(model_response: str) -> dict:
    """Extract JSON from model response"""
    match = re.search(r"\{.*\}", model_response, re.DOTALL)
    if not match:
        raise ValueError("No JSON found in model response")
    return json.loads(match.group(0))

def save_project_questions(parsed_json: dict, project_id: str, user_id: str):
    """
    Insert or update questions per (project_id, user_id, question_text).
    Prevent duplicates by checking existing questions first.
    """
    table = dynamodb.Table(PROJECT_QUESTIONS_TABLE)
    timestamp = datetime.utcnow().isoformat()
 
    print(f"\n{'='*80}")
    print(f"STARTING save_project_questions")
    print(f"project_id: {project_id}")
    print(f"user_id: {user_id}")
    print(f"{'='*80}\n")
 
    # Query all existing questions for this project
    existing_items = {}
    try:
        response = table.query(
            KeyConditionExpression=Key("project_id").eq(project_id)
        )
        print(f"EXISTING QUESTIONS IN DATABASE:")
        print(f"-" * 80)
        # Build a map of NORMALIZED question_text -> (question_id, original_text) for this user
        for item in response.get("Items", []):
            if item.get("user_id") == user_id:
                q_text = item.get("question_text", "")
                normalized_q = q_text.lower().strip()
                existing_items[normalized_q] = {
                    "question_id": item["question_id"],
                    "original_text": q_text
                }
                print(f"  • {q_text}")
        print(f"\nTotal existing questions for this user: {len(existing_items)}")
        print(f"{'='*80}\n")
    except Exception as e:
        print(f"ERROR querying existing questions: {e}")
        print(traceback.format_exc())
 
    # Process each question from client
    for question_text, answer_text in parsed_json.items():
        # Skip invalid answers
        if not (isinstance(answer_text, str) and answer_text.strip() and answer_text.strip().lower() != "not found"):
            continue
 
        # Normalize for comparison only
        normalized_question = question_text.lower().strip()
        print(f"\nPROCESSING QUESTION FROM CLIENT:")
        print(f"  Question: {question_text}")
        if normalized_question in existing_items:
            # UPDATE existing item
            question_id = existing_items[normalized_question]["question_id"]
            existing_q_text = existing_items[normalized_question]["original_text"]
            print(f"  ✅ MATCH FOUND - UPDATING EXISTING RECORD")
            print(f"     Existing question in DB: {existing_q_text}")
            print(f"     Question ID: {question_id}")
            try:
                table.update_item(
                    Key={
                        "project_id": project_id,
                        "question_id": question_id
                    },
                    UpdateExpression="SET answer_text = :at, #s = :st, updatedAt = :ua",
                    ExpressionAttributeNames={
                        "#s": "status"
                    },
                    ExpressionAttributeValues={
                        ":at": answer_text.strip(),
                        ":st": "completed",
                        ":ua": timestamp
                    }
                )
                print(f"  ✅ UPDATE SUCCESSFUL\n")
            except Exception as e:
                print(f"  ❌ UPDATE FAILED: {e}\n")
 
        else:
            # INSERT new item
            question_id = str(uuid.uuid4())
            print(f"  ➕ NO MATCH - INSERTING NEW RECORD")
            print(f"     New Question ID: {question_id}")
            try:
                table.put_item(
                    Item={
                        "project_id": project_id,
                        "question_id": question_id,
                        "user_id": user_id,
                        "question_text": question_text.strip(),
                        "answer_text": answer_text.strip(),
                        "status": "completed",
                        "createdAt": timestamp,
                        "updatedAt": timestamp
                    }
                )
                print(f"  ✅ INSERT SUCCESSFUL\n")
            except Exception as e:
                print(f"  ❌ INSERT FAILED: {e}\n")
    print(f"{'='*80}")
    print(f"COMPLETED save_project_questions")
    print(f"{'='*80}\n")  

def save_project_questions_old_old(parsed_json: dict, project_id: str, user_id: str):
    """Insert new question or update the existing one based on question_text"""
    table = dynamodb.Table(PROJECT_QUESTIONS_TABLE)
    timestamp = datetime.utcnow().isoformat()

    for question_text, answer_text in parsed_json.items():
        if not (isinstance(answer_text, str) and answer_text.strip().lower() != "not found"):
            continue

        # 1️⃣ Query GSI for existing record
        response = table.query(
            IndexName="project_id-question_text-index",
            KeyConditionExpression=Key("project_id").eq(project_id) & Key("question_text").eq(question_text)
        )

        if response["Count"] > 0:
            # 2️⃣ Update existing record
            existing_item = response["Items"][0]
            question_id = existing_item["question_id"]

            table.update_item(
                Key={
                    "project_id": project_id,
                    "question_id": question_id
                },
                UpdateExpression="""
                    SET answer_text = :at,
                    #st = :st,
                    updatedAt = :ua,
                    user_id = :uid
                """,
                ExpressionAttributeNames={
                    "#st": "status"
                },
                ExpressionAttributeValues={
                    ":at": answer_text,
                    ":st": "completed",
                    ":ua": timestamp,
                    ":uid": user_id
                }
            )
        else:
            # 3️⃣ Insert new record only if not exists
            table.put_item(
                Item={
                    "project_id": project_id,
                    "question_id": str(uuid.uuid4()),
                    "question_text": question_text,
                    "answer_text": answer_text,
                    "status": "completed",
                    "createdAt": timestamp,
                    "updatedAt": timestamp,
                    "user_id": user_id
                },
                ConditionExpression="attribute_not_exists(project_id) AND attribute_not_exists(question_id)"  
            )

def save_project_questions_old1(parsed_json: dict, project_id: str, user_id: str):
    """Save answered questions to database"""
    table = dynamodb.Table(PROJECT_QUESTIONS_TABLE)
    timestamp = datetime.utcnow().isoformat()
    
    for question_text, answer_text in parsed_json.items():
        if isinstance(answer_text, str) and answer_text.strip().lower() != "not found":
            table.put_item(
                Item={
                    "project_id": project_id,
                    "question_id": str(uuid.uuid4()),
                    "question_text": question_text,
                    "answer_text": answer_text,
                    "status": "completed",
                    "createdAt": timestamp,
                    "updatedAt": timestamp,
                    "user_id": user_id
                }
            )

def save_project_questions_old(parsed_json: dict, project_id: str, user_id: str):
    """Save answered questions to database (update if exists, insert if new)"""
    table = dynamodb.Table(PROJECT_QUESTIONS_TABLE)
    timestamp = datetime.utcnow().isoformat()

    for question_text, answer_text in parsed_json.items():
        if not isinstance(answer_text, str):
            continue

        # Only process answered questions
        if answer_text.strip().lower() != "not found":
            # 1. Check if this question already exists for the project
            resp = table.scan(
                FilterExpression=Attr("project_id").eq(project_id) & Attr("question_text").eq(question_text),
                ProjectionExpression="question_id"
            )
            items = resp.get("Items", [])

            if items:
                # 2. Update existing record
                existing_qid = items[0]["question_id"]

                table.update_item(
                    Key={
                        "project_id": project_id,
                        "question_id": existing_qid
                    },
                    UpdateExpression="""
                        SET answer_text = :a,
                            status = :s,
                            updatedAt = :u,
                            user_id = :uid
                    """,
                    ExpressionAttributeValues={
                        ":a": answer_text,
                        ":s": "completed",
                        ":u": timestamp,
                        ":uid": user_id
                    }
                )
            else:
                # 3. Insert new record
                table.put_item(
                    Item={
                        "project_id": project_id,
                        "question_id": str(uuid.uuid4()),
                        "question_text": question_text,
                        "answer_text": answer_text,
                        "status": "completed",
                        "createdAt": timestamp,
                        "updatedAt": timestamp,
                        "user_id": user_id
                    }
                )            

# ---------- Route Handlers ----------
def handle_connect(event, context):
    """Handle WebSocket connection"""
    connection_id = event["requestContext"]["connectionId"]
    print(f"New connection: {connection_id}")
    
    # We don't store anything here since we don't have session_id yet
    # The connection will be stored when startProcessing is called
    
    return {"statusCode": 200}

def handle_disconnect(event, context):
    """Handle WebSocket disconnection"""
    connection_id = event["requestContext"]["connectionId"]
    print(f"Disconnection: {connection_id}")
    
    # Remove the connection from user record
    remove_connection_from_user(connection_id)
    
    return {"statusCode": 200}

def handle_start_processing(event, context):
    """Handle the main processing request"""
    connection_id = event["requestContext"]["connectionId"]
    apigw = get_apigw_client(event)
    
    try:
        # Parse the message
        body = event.get("body", "{}")
        message = json.loads(body)
        
        session_id = message.get("session_id")
        project_id = message.get("project_id")
        document_type = message.get("document_type")
        text = message.get("text", "").strip()
        
        print(f"Processing request - Session: {session_id}, Project: {project_id}")
        
        # Validate required fields
        if not session_id or not project_id or not text:
            send_to_client(apigw, connection_id, {
                "error": "Missing required fields: session_id, project_id, or text"
            })
            return {"statusCode": 400}
        
        # Store connection_id in user record
        user_email = store_connection_in_user(session_id, connection_id)
        print(f"Stored connection for user: {user_email}")
        
        # Send processing started message
        send_to_client(apigw, connection_id, {
            "status": "processing_started",
            "message": "Processing your document..."
        })
        
        # Get user info
        user_info = get_user_by_session(session_id)
        user_id = user_info.get("id", user_info["email"])  # Use id if available, otherwise email
        
        # Send progress update
        send_to_client(apigw, connection_id, {
            "status": "analyzing_document",
            "message": "Analyzing document with AI..."
        })

        # Process with Bedrock
        if document_type == 'icp':
            bedrock_response = invoke_bedrock_icp(text, session_id)
        elif document_type == 'kmf':
            bedrock_response = invoke_bedrock_kmf(text, session_id)
        elif document_type == 'bs':
            bedrock_response = invoke_bedrock_bs(text, session_id)
        elif document_type == 'sr':
            bedrock_response = invoke_bedrock_sr(text, session_id) 
        else:
            send_to_client(apigw, connection_id, {
            "status": "error",
            "error": f"Unsupported document_type: {document_type}. Expected one of ['icp', 'kmf', 'bs', 'sr']."
            })
            return {"statusCode": 400}                           
        
        # Process with Bedrock
        # bedrock_response = invoke_bedrock(text, session_id)
        parsed_json = extract_json(bedrock_response)
        
        # Save answered questions to database
        save_project_questions(parsed_json, project_id, user_id)
        
        # Send not found questions to client immediately
        not_found_questions = []
        for question, answer in parsed_json.items():
            if isinstance(answer, str) and answer.strip().lower() == "not found":
                not_found_questions.append({
                    "question": question,
                    "answer": answer
                })
        
        if not_found_questions:
            send_to_client(apigw, connection_id, {
                "status": "questions_need_answers",
                "not_found_questions": not_found_questions
            })
        
        # Send final results
        answered_count = len([v for v in parsed_json.values() 
                            if isinstance(v, str) and v.strip().lower() != "not found"])
        unanswered_count = len(not_found_questions)
        
        final_result = {
            "status": "processing_complete",
            "message": "Document processing completed successfully",
            "session_id": session_id,
            "project_id": project_id,
            "answered_questions_count": answered_count,
            "unanswered_questions_count": unanswered_count,
            "results": parsed_json,
            "bedrock_response": bedrock_response
        }
        
        send_to_client(apigw, connection_id, final_result)
        
        return {"statusCode": 200}
        
    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        print(f"Error in processing: {error_msg}")
        print(traceback.format_exc())
        
        send_to_client(apigw, connection_id, {
            "status": "error",
            "error": error_msg
        })
        
        return {"statusCode": 500}

def handle_default(event, context):
    """Handle unknown routes"""
    connection_id = event["requestContext"]["connectionId"]
    apigw = get_apigw_client(event)
    
    send_to_client(apigw, connection_id, {
        "error": "Unknown action. Use 'startProcessing' action."
    })
    
    return {"statusCode": 400}

# ---------- Main Lambda Handler ----------
def lambda_handler(event, context):
    """Main WebSocket Lambda handler"""
    print("=== WebSocket Event ===")
    print(json.dumps(event, indent=2))
    
    try:
        route_key = event["requestContext"]["routeKey"]
        print(f"Route: {route_key}")
        
        # Route to appropriate handler
        if route_key == "$connect":
            return handle_connect(event, context)
        elif route_key == "$disconnect":
            return handle_disconnect(event, context)
        elif route_key == "startProcessing":
            return handle_start_processing(event, context)
        else:
            return handle_default(event, context)
            
    except Exception as e:
        print(f"Fatal error in lambda_handler: {str(e)}")
        print(traceback.format_exc())
        return {"statusCode": 500}