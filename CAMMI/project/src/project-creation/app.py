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
    "business.description_long": "Detailed business description",
    "product.core_offering": "Main product or service",
    "customer.primary_customer": "Primary target customer",
    "market.competitors": "Direct competitors",
    "strategy.short_term_goals": "Goals for next 12 months"
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

    prompt = f"""
Extract structured business facts ONLY from user text.

AVAILABLE FACT IDs:
{fact_list}

USER MESSAGE:
{answer_text}

Return STRICT JSON:
{{
  "extracted_facts":[
    {{"fact_id":"business.name","value":"text"}}
  ]
}}
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