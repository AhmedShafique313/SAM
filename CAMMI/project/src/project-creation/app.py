import json
import uuid
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key
from typing import List, Dict

# ======================================================
# AWS CLIENTS
# ======================================================
dynamodb = boto3.resource("dynamodb")

bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name=os.environ.get("BEDROCK_REGION", "us-east-1")
)

# ======================================================
# TABLES
# ======================================================
PROJECTS_TABLE_NAME = "projects-table"
USERS_TABLE_NAME = "users-table"
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")

projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)
facts_table = dynamodb.Table(FACTS_TABLE_NAME)

BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID",
    "anthropic.claude-3-sonnet-20240229-v1:0"
)

# ======================================================
# FACT UNIVERSE
# ======================================================
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
    "assets.spokesperson_role": "Spokesperson role or title"
}

# ======================================================
# RESPONSE
# ======================================================
def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
        },
        "body": json.dumps(body)
    }

# ======================================================
# BEDROCK CALL
# ======================================================
def invoke_claude(system_prompt, user_prompt):

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "temperature": 0.2,
        "system": system_prompt,
        "messages": [{"role": "user", "content": user_prompt}]
    }

    res = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps(payload)
    )

    data = json.loads(res["body"].read())
    return data["content"][0]["text"]

# ======================================================
# FACT EXTRACTION
# ======================================================
def extract_facts(answer_text: str) -> List[Dict]:

    fact_list = "\n".join(
        [f"- {fid}: {desc}" for fid, desc in FACT_UNIVERSE.items()]
    )

    prompt = fprompt = f"""
You are a strict information extraction engine.

Your task is to extract structured business facts from a user's description.

RULES:
1. Extract facts ONLY if they are explicitly stated or clearly implied.
2. DO NOT invent or assume information.
3. If the text does not contain a fact, DO NOT include it.
4. Use ONLY the FACT IDs listed below.
5. Each fact must appear only once.
6. Keep the value concise and human readable.
7. If the text is vague (example: "AI startup"), extract only what is certain.

FACT IDS AND DEFINITIONS:
{fact_list}

USER TEXT:
\"\"\"
{answer_text}
\"\"\"

OUTPUT FORMAT:
Return ONLY valid JSON. No explanation. No markdown.

{{
  "extracted_facts": [
    {{
      "fact_id": "fact.id.from.list",
      "value": "extracted value"
    }}
  ]
}}

EXAMPLES

Example 1
User text:
"We are an AI startup helping e-commerce companies automate marketing campaigns."

Output:
{{
  "extracted_facts": [
    {{"fact_id":"business.stage","value":"startup"}},
    {{"fact_id":"business.industry","value":"AI / marketing technology"}},
    {{"fact_id":"product.core_offering","value":"AI marketing automation platform"}},
    {{"fact_id":"customer.industries","value":"e-commerce companies"}}
  ]
}}

Example 2
User text:
"Mature SaaS company launching a new analytics product for fintech firms."

Output:
{{
  "extracted_facts":[
    {{"fact_id":"business.stage","value":"mature"}},
    {{"fact_id":"product.type","value":"analytics software"}},
    {{"fact_id":"customer.industries","value":"fintech"}}
  ]
}}

Now extract facts from the USER TEXT.
"""

    raw = invoke_claude(
        "You are a deterministic fact extraction engine.",
        prompt
    ).strip()

    if raw.startswith("```"):
        raw = raw.split("```")[1].strip()

    parsed = json.loads(raw)
    return parsed.get("extracted_facts", [])

# ======================================================
# FACT UPSERT
# ======================================================
def upsert_fact(project_id, fact_id, value):

    facts_table.update_item(
        Key={
            "project_id": project_id,
            "fact_id": fact_id
        },
        UpdateExpression="""
            SET #v=:v,
                #src=:src,
                #updated=:updated
        """,
        ExpressionAttributeNames={
            "#v": "value",
            "#src": "source",
            "#updated": "updated_at"
        },
        ExpressionAttributeValues={
            ":v": value,
            ":src": "chat",
            ":updated": datetime.utcnow().isoformat()
        }
    )

# ======================================================
# MAIN HANDLER
# ======================================================
def lambda_handler(event, context):

    try:

        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_name = body.get("project_name")
        answer_text = body.get("answer_text")

        if not session_id or not project_name:
            return response(400, "session_id and company_name required")

        project_name = project_name.strip()

        # ------------------------------------------------
        # 1. USER LOOKUP
        # ------------------------------------------------
        user_lookup = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id),
            Limit=1
        )

        if not user_lookup["Items"]:
            return response(404, "User not found")

        user_id = user_lookup["Items"][0]["id"]

        # ------------------------------------------------
        # 2. VALIDATION: COMPANY NAME UNIQUE PER USER
        # ------------------------------------------------
        existing_projects = projects_table.query(
            IndexName="user_id-index",
            KeyConditionExpression=Key("user_id").eq(user_id)
        )

        for item in existing_projects.get("Items", []):
            if item.get("project_name", "").strip().lower() == project_name.lower():
                return response(409, {
                    "message": "Company name already exists for this user"
                })

        # ------------------------------------------------
        # 3. CREATE PROJECT
        # ------------------------------------------------
        project_id = str(uuid.uuid4())

        item = {
            "id": project_id,
            "createdAt": datetime.utcnow().isoformat(),
            "session_id": session_id,
            "user_id": user_id,
            "project_name": project_name
        }

        projects_table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(id)"
        )

        facts_saved = []

        # ------------------------------------------------
        # OPTIONAL FACT EXTRACTION
        # ------------------------------------------------
        if answer_text:

            extracted_facts = extract_facts(answer_text)

            for fact in extracted_facts:
                upsert_fact(
                    project_id,
                    fact["fact_id"],
                    fact["value"]
                )

            facts_saved = extracted_facts

        # ------------------------------------------------
        # RESPONSE (PROJECT → COMPANY)
        # ------------------------------------------------
        return response(201, {
            "project_id": project_id,
            "project_name": project_name,
            "user_id": user_id,
            "facts_saved": facts_saved,
            "count": len(facts_saved)
        })

    except projects_table.meta.client.exceptions.ConditionalCheckFailedException:
        return response(409, "Company already exists")

    except Exception as e:
        print("ERROR:", str(e))
        return response(500, "Internal server error")