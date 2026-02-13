import json
import boto3
import pdfplumber
import io
import ast
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# Constants
BUCKET_NAME = "cammi-devprod"
USERS_TABLE = "users-table"
FACTS_TABLE = "facts-table" 

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

def parse_fact_universe(parsed_profile: str) -> dict:
    text = parsed_profile.strip()

    # remove appended sections if present
    if "--- NEW PDF EXTRACT ---" in text:
        text = text.split("--- NEW PDF EXTRACT ---")[0]

    # remove FACT_UNIVERSE =
    if "FACT_UNIVERSE" in text:
        text = text.split("FACT_UNIVERSE")[-1]

    start = text.find("{")
    end = text.rfind("}") + 1
    dict_str = text[start:end]

    return ast.literal_eval(dict_str)

# 🧾 Prompt builder for business profile extraction
def call_llm_extract_profile(all_content: str) -> str:
    prompt_relevancy = f"""
You are a senior B2B business analyst and structured data extractor.

You are given structured company information (scraped and pre-organized in JSON or markdown):

INPUT:
{str(all_content)}

YOUR TASK:
Extract factual information only from the input and populate the FACT_UNIVERSE dictionary.

STRICT RULES — MUST FOLLOW:
- Return output as a valid Python dictionary only.
- Do NOT wrap the output in markdown.
- Do NOT use code fences.
- Do NOT include ``` or ```python anywhere.
- Do NOT include explanations or comments.
- Do NOT include any text before or after the dictionary.
- Use EXACT keys as provided — do not rename, modify, or reorder keys.
- Do NOT add new keys.
- Do NOT remove keys.
- Values must come only from the input.
- If unknown, return empty string "".

OUTPUT FORMAT — RETURN EXACTLY THIS STRUCTURE WITH FILLED VALUES:

FACT_UNIVERSE = {{
    "business.name": "",
    "business.description_short": "",
    "business.description_long": "",
    "business.industry": "",
    "business.stage": "",
    "business.business_model": "",
    "business.pricing_position": "",
    "business.geography": "",
    "business.start_date": "",
    "business.end_date_or_milestone": "",
    "product.type": "",
    "product.core_offering": "",
    "product.value_proposition_short": "",
    "product.value_proposition_long": "",
    "product.problems_solved": "",
    "product.unique_differentiation": "",
    "product.strengths": "",
    "product.weaknesses": "",
    "customer.primary_customer": "",
    "customer.buyer_roles": "",
    "customer.user_roles": "",
    "customer.decision_maker": "",
    "customer.buyer_goals": "",
    "customer.buyer_pressures": "",
    "customer.industries": "",
    "customer.company_size": "",
    "customer.geography": "",
    "customer.information_sources": "",
    "customer.problems": "",
    "customer.pains": "",
    "customer.current_solutions": "",
    "customer.solution_gaps": "",
    "market.competitors": "",
    "market.alternatives": "",
    "market.why_alternatives_fail": "",
    "market.market_size_estimate": "",
    "market.trends_or_shifts": "",
    "market.opportunities": "",
    "market.threats": "",
    "strategy.short_term_goals": "",
    "strategy.long_term_vision": "",
    "strategy.success_definition": "",
    "strategy.priorities": "",
    "strategy.gtm_focus": "",
    "strategy.marketing_objectives": "",
    "strategy.user_growth_priorities": "",
    "strategy.marketing_tools": "",
    "strategy.marketing_budget": "",
    "brand.mission": "",
    "brand.vision": "",
    "brand.tone_personality": "",
    "brand.values_themes": "",
    "brand.vibes_to_avoid": "",
    "brand.key_messages": "",
    "revenue.pricing_position": "",
    "revenue.average_contract_value": "",
    "revenue.market_size": "",
    "revenue.marketing_budget": "",
    "assets.approved_customers": "",
    "assets.case_studies": "",
    "assets.videos": "",
    "assets.logos": "",
    "assets.quotes": "",
    "assets.brag_points": "",
    "assets.visual_assets": "",
    "assets.spokesperson_name": "",
    "assets.spokesperson_role": ""
}}

REMINDER:
Return ONLY the populated FACT_UNIVERSE dictionary.
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

    facts_dict = parse_fact_universe(parsed_profile)
    facts_table = dynamodb.Table(FACTS_TABLE)
    now_iso = datetime.now(timezone.utc).isoformat()

    for fact_id, value in facts_dict.items():
        # ✅ skip empty values
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue

        facts_table.update_item(
            Key={
                "project_id": project_id,
                "fact_id": fact_id
            },
            UpdateExpression="SET #v = :v, #s = :s, #u = :u",
            ExpressionAttributeNames={
                "#v": "value",
                "#s": "source",
                "#u": "updated_at"
            },
            ExpressionAttributeValues={
                ":v": str(value),
                ":s": "pdf",
                ":u": now_iso
            }
        )

    print("Facts saved to DynamoDB (non-empty values only).")

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
    knowledgebase_output = f"knowledgebase/{user_id}/{user_id}_data.txt"

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

    common_metadata = {
    "user_id": user_id,
    "project_id": project_id
    }


    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Metadata=common_metadata,
        Body=final_output.encode("utf-8"),
        ContentType="text/plain"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=knowledgebase_output,
        Metadata=common_metadata,
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
