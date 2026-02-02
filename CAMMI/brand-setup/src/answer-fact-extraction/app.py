import json
import boto3
import os
from datetime import datetime
from typing import List, Dict

# ---------------------------
# AWS Clients
# ---------------------------
dynamodb = boto3.resource("dynamodb")
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name=os.environ.get("BEDROCK_REGION", "us-east-1")
)

# ---------------------------
# Environment
# ---------------------------
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID",
    "anthropic.claude-3-sonnet-20240229-v1:0"
)

facts_table = dynamodb.Table(FACTS_TABLE_NAME)

# ---------------------------
# FACT UNIVERSE (example – keep full version from your code)
# ---------------------------
FACT_UNIVERSE = {
    "business.name": "Legal or common name of the business",
    "business.description_long": "Detailed business description",
    "product.core_offering": "Main product or service",
    "customer.primary_customer": "Primary target customer",
    "market.competitors": "Direct competitors",
    "strategy.short_term_goals": "Goals for next 12 months"
}

# ---------------------------
# CORS Response Helper
# ---------------------------
def response(status: int, body: dict):
    return {
        "statusCode": status,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST"
        },
        "body": json.dumps(body)
    }

# ---------------------------
# Bedrock Invocation
# ---------------------------
def invoke_claude(system_prompt: str, user_prompt: str) -> str:
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.2,
        "system": system_prompt,
        "messages": [
            {"role": "user", "content": user_prompt}
        ]
    }

    res = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(payload)
    )

    data = json.loads(res["body"].read())
    return data["content"][0]["text"]

# ---------------------------
# Industry-Standard Fact Extraction
# ---------------------------
def extract_facts(question_text: str) -> List[Dict[str, str]]:
    fact_list = "\n".join(
        [f"- {fid}: {desc}" for fid, desc in FACT_UNIVERSE.items()]
    )

    prompt = f"""
You are the FACT EXTRACTION layer of a production business-intelligence system.

Your ONLY responsibility is to extract high-confidence, structured business facts
from the user's latest message.

────────────────────────────────
STRICT EXTRACTION RULES

1. Extract ONLY facts explicitly stated in the LATEST user message
2. DO NOT infer, guess, assume, or complete missing information
3. DO NOT extract vague, uncertain, or implied information
4. DO NOT extract opinions, intentions, or future possibilities
5. DO NOT rewrite or normalize the user’s language unnecessarily
6. Use ONLY the provided fact_id list
7. NEVER invent new fact_ids
8. If no facts can be extracted with high confidence, return an empty list
9. If a fact value is ambiguous, DO NOT extract it

────────────────────────────────
AVAILABLE FACT IDs

{fact_list}

────────────────────────────────
LATEST USER MESSAGE

{question_text}

────────────────────────────────
OUTPUT FORMAT (STRICT JSON)

{{
  "extracted_facts": [
    {{
      "fact_id": "business.name",
      "value": "Exact phrase used by the user"
    }}
  ]
}}

If no facts are extractable:

{{
  "extracted_facts": []
}}

────────────────────────────────
IMPORTANT

- Output ONLY valid JSON
- No markdown
- No explanations
- No commentary
"""

    raw = invoke_claude(
        system_prompt="You are a deterministic fact extraction engine.",
        user_prompt=prompt
    ).strip()

    # Defensive cleanup
    if raw.startswith("```"):
        raw = raw.split("```")[1].strip()

    parsed = json.loads(raw)
    return parsed.get("extracted_facts", [])

# ---------------------------
# DynamoDB Upsert
# ---------------------------
def upsert_fact(project_id: str, fact_id: str, value: str):
    facts_table.update_item(
        Key={
            "project_id": project_id,
            "fact_id": fact_id
        },
        UpdateExpression="""
            SET #val = :val,
                #src = :src,
                #updated = :updated
        """,
        ExpressionAttributeNames={
            "#val": "value",
            "#src": "source",
            "#updated": "updated_at"
        },
        ExpressionAttributeValues={
            ":val": value,
            ":src": "chat",
            ":updated": datetime.utcnow().isoformat()
        }
    )

# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):

    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    try:
        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_id = body.get("project_id")
        question_text = body.get("question_text")

        if not all([session_id, project_id, question_text]):
            return response(400, {
                "error": "session_id, project_id, and question_text are required"
            })

        # Extract facts
        extracted_facts = extract_facts(question_text)

        # Persist facts
        for fact in extracted_facts:
            upsert_fact(
                project_id=project_id,
                fact_id=fact["fact_id"],
                value=fact["value"]
            )

        return response(200, {
            "session_id": session_id,
            "project_id": project_id,
            "facts_saved": extracted_facts,
            "count": len(extracted_facts)
        })

    except Exception as e:
        print("ERROR:", str(e))
        return response(500, {
            "error": "Internal server error",
            "details": str(e)
        })
