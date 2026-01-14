import json
import boto3
import os
from boto3.dynamodb.conditions import Key

# AWS clients
s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

# Resources
BUCKET_NAME = "cammi-devprod"
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("campaigns-table")

DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


def llm_calling(prompt: str, model_id: str):
    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ],
        inferenceConfig={
            "maxTokens": 4000,
            "temperature": 0.6,
            "topP": 0.9
        }
    )
    return response["output"]["message"]["content"][0]["text"]


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    session_id = body.get("session_id")
    campaign_name = body.get("campaign_name")
    campaign_goal_type = body.get("campaign_goal_type")
    platform_name = body.get("platform_name")

    if not all([session_id, campaign_name, campaign_goal_type, platform_name]):
        return build_response(400, {"error": "Missing required fields"})

    # ------------------------------------------------------------------
    # 1. Resolve user_id using session_id (via GSI)
    # ------------------------------------------------------------------
    user_resp = users_table.query(
        IndexName="user_id_index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user = user_resp["Items"][0]
    user_id = user["user_id"]

    # ------------------------------------------------------------------
    # 2. Store campaign names under user record (String Set)
    # ------------------------------------------------------------------
    users_table.update_item(
        Key={"email": user["email"]},
        UpdateExpression="ADD campaigns :c",
        ExpressionAttributeValues={
            ":c": {campaign_name}
        }
    )

    # ------------------------------------------------------------------
    # 3. Read campaign context from S3 (no try/except)
    # ------------------------------------------------------------------
    s3_key = f"execution - ready campaigns/{campaign_name}/data.txt"

    s3_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    campaign_context = s3_obj["Body"].read().decode("utf-8")

    # ------------------------------------------------------------------
    # 4. LLM Prompt (STRICT JSON OUTPUT)
    # ------------------------------------------------------------------
    prompt = f"""
You are a senior paid media and brand strategist.

Campaign Goal Type:
{campaign_goal_type}

Primary Platform:
{platform_name}

Campaign Context:
{campaign_context}

TASK:
Generate a campaign execution plan.

OUTPUT RULES:
- Return ONLY valid JSON.
- Do NOT include explanations or markdown.
- Do NOT add extra keys.

Required JSON format:
{{
  "campaign_duration_days": number,
  "best_suited_platform": string,
  "campaign_type": {{
    "brand_tone": string,
    "brand_voice": string,
    "key_message": string
  }},
  "best_posting_time": string,
  "creative_brief": string
}}
"""

    llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)

    generated_campaign = json.loads(llm_response)

    # ------------------------------------------------------------------
    # 5. Save campaign data in campaigns-table
    # ------------------------------------------------------------------
    campaigns_table.put_item(
        Item={
            "user_id": user_id,
            "campaign_name": campaign_name,
            "platform_name": platform_name,
            "campaign_goal_type": campaign_goal_type,
            "generated_campaign": generated_campaign
        }
    )

    # ------------------------------------------------------------------
    # 6. Frontend response
    # ------------------------------------------------------------------
    return build_response(200, {
        "message": "Cammi is analyzing your input",
        "data": generated_campaign
    })


def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body)
    }
