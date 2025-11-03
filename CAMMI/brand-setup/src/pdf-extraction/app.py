import json
import boto3
import pdfplumber
import io
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# Constants
BUCKET_NAME = "cammi"
USERS_TABLE = "Users"

# 🧠 Bedrock LLM call
def llm_calling(prompt, model_id, session_id="default-session"):
    """Call AWS Bedrock LLM. (No try/except — errors will propagate for visibility.)"""
    conversation = [
        {
            "role": "user",
            "content": [{"text": str(prompt)}]
        }
    ]

    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=conversation,
        inferenceConfig={
            "temperature": 0.7,
            "topP": 0.9
        },
        requestMetadata={
            "sessionId": session_id
        }
    )

    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text.strip()

# 🧾 Prompt builder for business profile extraction
def call_llm_extract_profile(all_content: str) -> str:
    prompt_relevancy = f"""
You are an expert business and marketing analyst specializing in B2B brand strategy.
 
You are given structured company information (scraped and pre-organized in JSON or markdown):
{str(all_content)}
 
Your task:
1. Extract all key information relevant to building a detailed and personalized business profile.
2. Use only factual data found in the input. Do not infer or invent data.
3. Return the response in the exact format below using the same headings and order.
4. If any field cannot be determined confidently, leave it blank (do not make assumptions).
 
Return your answer in this format exactly:
 
Business Name:
Industry / Sector:
Mission:
Vision:
Objective / Purpose Statement:
Business Concept:
Products or Services Offered:
Target Market:
Who They Currently Sell To:
Value Proposition:
Top Business Goals:
Challenges:
Company Overview / About Summary:
Core Values / Brand Personality:
Unique Selling Points (USPs):
Competitive Advantage / What Sets Them Apart:
Market Positioning Statement:
Customer Segments:
Proof Points / Case Studies / Testimonials Summary:
Key Differentiators:
Tone of Voice / Brand Personality Keywords:
Core Features / Capabilities:
Business Model:
Technology Stack / Tools / Platform:
Geographic Presence:
Leadership / Founder Info:
Company Values / Culture:
Strategic Initiatives / Future Plans:
Awards / Recognition / Partnerships:
Press Mentions or Achievements:
Client or Industry Verticals Served:
 
Notes:
- Keep responses concise and factual.
- Avoid any assumptions or generation of new data.
- Use sentence form, not bullet lists, except where lists are explicitly more natural.
    """.strip()

    return llm_calling(prompt_relevancy, model_id="us.anthropic.claude-sonnet-4-20250514-v1:0")

# 🧩 Lambda entrypoint (triggered by S3 PUT event)
def lambda_handler(event, context):
    print("Received S3 event:", json.dumps(event))

    # Extract bucket and object key from event
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    # Ensure correct bucket
    if bucket_name != BUCKET_NAME:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Unexpected bucket: {bucket_name}"})
        }

    # ✅ Extract project_id and session_id from object_key
    # Expected pattern: pdf_files/{project_id}/{session_id}/{file_name}
    parts = object_key.split("/")
    if len(parts) < 4 or parts[0] != "pdf_files":
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Invalid S3 key structure: {object_key}"})
        }

    project_id = parts[1]
    session_id = parts[2]
    file_name = parts[3]

    print(f"Extracted project_id: {project_id}, session_id: {session_id}, file_name: {file_name}")

    # ✅ Fetch PDF file from S3
    pdf_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    pdf_bytes = pdf_obj["Body"].read()

    print(f"Reading file from S3: {object_key}")
    print(f"File size: {len(pdf_bytes)} bytes")

    # ✅ Extract text from PDF
    all_text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"Total pages: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages):
            print(f"Extracting page {i + 1} ...")
            text = page.extract_text()
            if text:
                all_text += text + "\n" + "-" * 80 + "\n"

    # 🧠 Call Bedrock LLM
    print("Calling Bedrock LLM to generate structured business profile...")
    parsed_profile = call_llm_extract_profile(all_text)
    print("LLM processing completed.")

    # ✅ Get user_id from DynamoDB
    table = dynamodb.Table(USERS_TABLE)
    try:
        response = table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id)
        )
        if not response.get("Items"):
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"No user found for session_id {session_id}"})
            }
        user_id = response["Items"][0]["id"]
        print(f"User found via GSI: {user_id}")
    except Exception as e:
        print(f"GSI not found or query failed, fallback to scan: {e}")
        response = table.scan(
            FilterExpression=Attr("session_id").eq(session_id)
        )
        if not response.get("Items"):
            return {
                "statusCode": 404,
                "body": json.dumps({"message": f"No user found for session_id {session_id}"})
            }
        user_id = response["Items"][0]["id"]
        print(f"User found via scan: {user_id}")

    # ✅ Upload extracted + parsed text to S3 (append if exists)
    s3_key = f"url_parsing/{project_id}/{user_id}/web_scraping.txt"

    try:
        existing_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
        existing_content = existing_obj["Body"].read().decode("utf-8")
        print("Existing file found. Appending new content...")
    except s3.exceptions.NoSuchKey:
        existing_content = ""
        print("No existing file found. Creating a new one...")

    final_output = (
        existing_content + "\n\n--- NEW PDF EXTRACT ---\n\n" + parsed_profile
        if existing_content
        else parsed_profile
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=final_output.encode("utf-8"),
        ContentType="text/plain"
    )

    s3_url = f"s3://{BUCKET_NAME}/{s3_key}"

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Triggered by S3: PDF processed successfully and profile saved.",
            "project_id": project_id,
            "session_id": session_id,
            "url": s3_url
        })
    }
