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
PROJECTS_TABLE_NAME = os.environ.get("PROJECTS_TABLE_NAME", "projects-table")
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID",
    "anthropic.claude-3-sonnet-20240229-v1:0"
)

facts_table = dynamodb.Table(FACTS_TABLE_NAME)
projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)

# ---------------------------
# FACT UNIVERSE
# ---------------------------
FACT_UNIVERSE = {
    "business.name": "Legal or common name of the business",
    "business.description_short": "Brief one-line description",
    "business.description_long": "Detailed business description",
    "business.industry": "Primary industry or sector",
    "business.stage": "Stage of business (startup, growth, mature)",
    "business.business_model": "How the business makes money",
    "business.pricing_position": "Pricing strategy positioning",
    "business.geography": "Primary business location or market",
    "business.start_date": "Business start date or launch date",
    "business.end_date_or_milestone": "Target end date or major milestone",
    "product.type": "Type of product or service",
    "product.core_offering": "Main product or service offering",
    "product.value_proposition_short": "Brief value proposition",
    "product.value_proposition_long": "Detailed value proposition",
    "product.problems_solved": "Problems the product solves",
    "product.unique_differentiation": "What makes the product unique",
    "product.strengths": "Product or company strengths",
    "product.weaknesses": "Product or company weaknesses",
    "customer.primary_customer": "Primary target customer description",
    "customer.buyer_roles": "Roles of people who buy",
    "customer.user_roles": "Roles of people who use the product",
    "customer.decision_maker": "Who makes the final purchase decision",
    "customer.buyer_goals": "What buyers are trying to achieve",
    "customer.buyer_pressures": "Pressures or constraints on buyers",
    "customer.industries": "Industries of target customers",
    "customer.company_size": "Size of target customer companies",
    "customer.geography": "Geographic location of customers",
    "customer.information_sources": "Where customers find information",
    "customer.problems": "Key problems customers face",
    "customer.pains": "Specific pains or frustrations",
    "customer.current_solutions": "How customers solve problems today",
    "customer.solution_gaps": "Gaps in current solutions",
    "market.competitors": "Direct competitors",
    "market.alternatives": "Alternative solutions",
    "market.why_alternatives_fail": "Why alternatives don't work well",
    "market.market_size_estimate": "Estimated market size",
    "market.trends_or_shifts": "Market trends or shifts",
    "market.opportunities": "Market opportunities",
    "market.threats": "Market threats or risks",
    "strategy.short_term_goals": "Goals for next 12 months",
    "strategy.long_term_vision": "3-5 year vision",
    "strategy.success_definition": "How success is defined",
    "strategy.priorities": "Top strategic priorities",
    "strategy.gtm_focus": "Go-to-market focus and strategy",
    "strategy.marketing_objectives": "Marketing objectives",
    "strategy.user_growth_priorities": "User growth priorities",
    "strategy.marketing_tools": "Marketing tools and channels",
    "strategy.marketing_budget": "Marketing budget",
    "brand.mission": "Company mission statement",
    "brand.vision": "Company vision statement",
    "brand.tone_personality": "Brand tone and personality",
    "brand.values_themes": "Brand values and themes",
    "brand.vibes_to_avoid": "Brand vibes to avoid",
    "brand.key_messages": "Key brand messages",
    "revenue.pricing_position": "Pricing position in market",
    "revenue.average_contract_value": "Average contract value",
    "revenue.market_size": "Total addressable market size",
    "revenue.marketing_budget": "Marketing budget allocation",
    "assets.approved_customers": "Approved customer names",
    "assets.case_studies": "Available case studies",
    "assets.videos": "Video assets",
    "assets.logos": "Customer or partner logos",
    "assets.quotes": "Customer quotes or testimonials",
    "assets.brag_points": "Notable achievements",
    "assets.visual_assets": "Visual assets available",
    "assets.spokesperson_name": "Company spokesperson name",
    "assets.spokesperson_role": "Spokesperson role or title",
    "content.calendar_timeframe": "Time period the content calendar covers",
    "content.special_activities": "Special events, launches, or activities to include in content calendar",
    "content.pillars": "Key content themes or pillars",
    "content.channels": "Content distribution channels (LinkedIn, blog, email, etc.)",
    "content.formats": "Content formats to be used (articles, infographics, videos, etc.)",
    "content.funnel_stages_priority": "Which funnel stages to prioritize (Top, Middle, Bottom)",
    "strategy.quarter_timeframe": "Specific quarter being planned (e.g., Q4 2024)",
    "strategy.quarterly_special_activities": "Major activities or launches planned for the quarter",
    "strategy.quarterly_goals": "Five specific measurable goals for the quarter",
    "strategy.kpi_targets": "Key performance indicator targets for quarterly goals",
    "strategy.marketing_team_structure": "Marketing team roles and owners for tactical execution"
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
# Project Existence Check
# ---------------------------
def project_exists(project_id: str) -> bool:
    resp = projects_table.get_item(
        Key={"id": project_id},
        ProjectionExpression="id"
    )
    return "Item" in resp

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

        # ---------------------------
        # NEW: Project existence guard
        # ---------------------------
        if not project_exists(project_id):
            return response(200, {
                "session_id": session_id,
                "project_id": project_id,
                "facts_saved": [],
                "count": 0,
                "message": "Project does not exist. No facts extracted."
            })

        # ---------------------------
        # Existing behavior (unchanged)
        # ---------------------------
        extracted_facts = extract_facts(question_text)

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
