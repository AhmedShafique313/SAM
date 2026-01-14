import json
import boto3
import pdfplumber
import io
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.client('s3')
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
BUCKET_NAME = "cammi-devprod"

def llm_calling(prompt, model_id):
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
        }
    )
    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text.strip()

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

def lambda_handler(event, context):
    print("Received S3 event:", json.dumps(event))
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    object_key = record["s3"]["object"]["key"]

    if bucket_name != BUCKET_NAME:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Unexpected bucket: {bucket_name}"})
        }

    # ✅ Extract project_id and session_id from object_key
    # Expected pattern: pdf_files/{project_id}/{session_id}/{file_name}
    parts = object_key.split("/")
    if len(parts) < 3 or parts[0] != "execution - ready campaigns":
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Invalid S3 key structure: {object_key}"})
        }
    
    campaign_name = parts[1]
    file_name = parts[2]

    print(f"Extracted campaign_name: {campaign_name}, file_name: {file_name}")

    pdf_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    pdf_bytes = pdf_obj["Body"].read()

    print(f"Reading file from S3: {object_key}")
    print(f"File size: {len(pdf_bytes)} bytes")

    all_text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"Total pages: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages):
            print(f"Extracting page {i + 1} ...")
            text = page.extract_text()
            if text:
                all_text += text + "\n" + "-" * 80 + "\n"

    print("Calling Bedrock LLM to generate structured business profile...")
    parsed_profile = call_llm_extract_profile(all_text)
    print("LLM processing completed.")

    s3_key = f"execution - ready campaigns/{campaign_name}/data.txt"

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
            "campaign_name": campaign_name,
            "url": s3_url
        })
    }
