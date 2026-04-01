import json
import boto3
import base64
import pdfplumber
import io
import ast
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

# -------------------------------------------------
# AWS Clients
# -------------------------------------------------
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# -------------------------------------------------
# Tables
# -------------------------------------------------
users_table = dynamodb.Table("users-table")
facts_table = dynamodb.Table("facts-table")

# -------------------------------------------------
# Constants
# -------------------------------------------------
BUCKET_NAME = "cammi-devprod"

# -------------------------------------------------
# USER HELPERS
# -------------------------------------------------
def get_user_by_session(session_id):
    """Get user from DynamoDB by session_id"""
    res = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )
    return res["Items"][0] if res.get("Items") else None


def update_user_credits(email, amount):
    """Update user credits in DynamoDB"""
    users_table.update_item(
        Key={"email": email},
        UpdateExpression="SET total_credits = :v",
        ExpressionAttributeValues={":v": amount},
    )

# -------------------------------------------------
# LLM HELPERS
# -------------------------------------------------
def llm_calling(prompt, model_id, session_id="default-session"):
    """Call AWS Bedrock LLM"""
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
    """Parse the LLM response to extract FACT_UNIVERSE dictionary"""
    text = parsed_profile.strip()

    # Remove appended sections if present
    if "--- NEW PDF EXTRACT ---" in text:
        text = text.split("--- NEW PDF EXTRACT ---")[0]

    # Remove FACT_UNIVERSE =
    if "FACT_UNIVERSE" in text:
        text = text.split("FACT_UNIVERSE")[-1]

    start = text.find("{")
    end = text.rfind("}") + 1
    dict_str = text[start:end]

    return ast.literal_eval(dict_str)


def call_llm_extract_profile(all_content: str) -> str:
    """Build prompt and call LLM to extract business profile"""
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

# -------------------------------------------------
# PDF PROCESSING
# -------------------------------------------------
def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using pdfplumber"""
    all_text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        print(f"Total pages: {len(pdf.pages)}")
        for i, page in enumerate(pdf.pages):
            print(f"Extracting page {i + 1} ...")
            text = page.extract_text()
            if text:
                all_text += text + "\n" + "-" * 80 + "\n"
    return all_text


def save_facts_to_dynamodb(facts_dict: dict, project_id: str):
    """Save extracted facts to DynamoDB facts-table"""
    now_iso = datetime.now(timezone.utc).isoformat()

    for fact_id, value in facts_dict.items():
        # Skip empty values
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


def upload_to_s3(project_id: str, user_id: str, parsed_profile: str):
    """Upload processed content to S3"""
    s3_key = f"url_parsing/{project_id}/{user_id}/web_scraping.txt"
    knowledgebase_output = f"knowledgebase/{user_id}/{user_id}_data.txt"

    # Check if existing file exists and append
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

    # Upload to both locations
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

    print(f"Content uploaded to S3: {s3_key} and {knowledgebase_output}")
    return f"s3://{BUCKET_NAME}/{s3_key}"

# -------------------------------------------------
# MAIN LAMBDA HANDLER
# -------------------------------------------------
def lambda_handler(event, context):
    try:
        print("Received event:", json.dumps(event))

        # Parse request body
        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_id = body.get("project_id")
        file_name = body.get("file_name")
        file_content = body.get("file_content")  # Base64 encoded PDF

        # Validate required parameters
        if not session_id or not project_id:
            return _response(400, {"error": "Missing session_id and project_id in request body"})

        if not file_content:
            return _response(400, {"error": "Missing file_content (base64 encoded PDF) in request body"})

        # ================= USER FETCH =================
        user = get_user_by_session(session_id)

        if not user:
            return _response(404, {"error": "User not found"})

        email = user["email"]
        user_id = user["id"]

        # ================= CREDIT CHECK =================
        credits = int(user.get("total_credits", 0))

        if credits < 2:
            return _response(402, {"error": "Insufficient credits. Required: 2, Available: " + str(credits)})

        # Deduct 2 credits
        new_credits = max(credits - 2, 0)
        update_user_credits(email, new_credits)
        print(f"Credits deducted. Remaining: {new_credits}")

        # ================= DECODE PDF =================
        try:
            pdf_bytes = base64.b64decode(file_content)
            print(f"PDF decoded. Size: {len(pdf_bytes)} bytes")
        except Exception as e:
            return _response(400, {"error": f"Invalid base64 file_content: {str(e)}"})

        # ================= EXTRACT TEXT FROM PDF =================
        print("Extracting text from PDF...")
        all_text = extract_text_from_pdf(pdf_bytes)
        print(f"Text extraction completed. Length: {len(all_text)} characters")

        # ================= CALL BEDROCK LLM =================
        print("Calling Bedrock LLM to generate structured business profile...")
        parsed_profile = call_llm_extract_profile(all_text)
        print("LLM processing completed.")

        # ================= PARSE FACTS =================
        print("Parsing facts from LLM response...")
        facts_dict = parse_fact_universe(parsed_profile)
        print(f"Parsed {len(facts_dict)} facts")

        # ================= SAVE TO DYNAMODB =================
        print("Saving facts to DynamoDB...")
        save_facts_to_dynamodb(facts_dict, project_id)

        # ================= UPLOAD TO S3 =================
        print("Uploading processed content to S3...")
        s3_url = upload_to_s3(project_id, user_id, parsed_profile)

        # ================= FILE NAME =================
        if not file_name:
            file_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.pdf"

        # ================= PREPARE RESPONSE =================
        response_body = {
            "message": "PDF processed successfully",
            "file_name": file_name,
            "session_id": session_id,
            "project_id": project_id,
            "user_id": user_id,
            "remaining_credits": new_credits,
            "facts_saved": len([v for v in facts_dict.values() if v]),
            "s3_url": s3_url
        }

        return _response(200, response_body)

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return _response(500, {"error": f"Internal server error: {str(e)}"})

# -------------------------------------------------
# RESPONSE HELPER
# -------------------------------------------------
def _response(status_code, body_obj):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
        },
        "body": json.dumps(body_obj)
    }
