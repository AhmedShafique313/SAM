import json
import boto3
import re
from boto3.dynamodb.conditions import Key
from datetime import datetime

# ---------------------------------------------------------
# AWS Clients
# ---------------------------------------------------------
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

# ---------------------------------------------------------
# DynamoDB Tables
# ---------------------------------------------------------
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("campaigns-table")

DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def llm_calling(prompt: str, model_id: str) -> str:
    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={
            "maxTokens": 8000,
            "temperature": 0.7,
            "topP": 0.9
        }
    )
    return response["output"]["message"]["content"][0]["text"]


def extract_json(text: str) -> dict:
    match = re.search(r"\{[\s\S]*\}", text)
    if not match:
        raise ValueError("No JSON found in LLM response")
    return json.loads(match.group())


# ---------------------------------------------------------
# Lambda Handler
# ---------------------------------------------------------
def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    session_id = body.get("session_id")
    campaign_name = body.get("campaign_name")
    campaign_goal_type = body.get("campaign_goal_type")
    platform_name = body.get("platform_name")

    data = body.get("data", {})
    post_volume = data.get("post_volume")
    best_posting_time = data.get("best_posting_time")
    creative_brief = data.get("creative_brief")

    if not all([
        session_id,
        campaign_name,
        campaign_goal_type,
        platform_name,
        post_volume,
        best_posting_time,
        creative_brief
    ]):
        return build_response(400, {"error": "Missing required fields"})

    total_posts = post_volume.get("total_posts")
    if not total_posts or total_posts <= 0:
        return build_response(400, {"error": "Invalid post_volume.total_posts"})

    # ---------------------------------------------------------
    # 1. Resolve user
    # ---------------------------------------------------------
    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user_id = user_resp["Items"][0]["id"]

    # ---------------------------------------------------------
    # 2. Prompt
    # ---------------------------------------------------------
    prompt = f"""
You are a senior social media strategist.

Generate EXACTLY {total_posts} posts.

STRICT RULES:
- Output MUST start with {{ and end with }}
- No explanations
- No markdown
- No text before or after JSON

JSON FORMAT:
{{
  "posts": [
    {{
      "title": string,
      "description": string,
      "caption": string,
      "hashtags": [string],
      "keywords": [string],
      "image_generation_prompt": string,
      "best_post_time": string,
      "best_post_day": string
    }}
  ]
}}

Platform: {platform_name}
Campaign Goal: {campaign_goal_type}
Posting Window: {best_posting_time}
Creative Brief: {creative_brief}
"""

    # ---------------------------------------------------------
    # 3. Generate & Parse
    # ---------------------------------------------------------
    llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)

    try:
        generated_posts = extract_json(llm_response)
        posts = generated_posts["posts"]
    except Exception:
        return build_response(500, {
            "error": "Invalid LLM response format",
            "raw_llm_output": llm_response[:1000]
        })

    if len(posts) != total_posts:
        return build_response(500, {"error": "Incorrect number of posts generated"})

    # ---------------------------------------------------------
    # 4. Save
    # ---------------------------------------------------------
    campaigns_table.put_item(
        Item={
            "user_id": user_id,
            "campaign_name": campaign_name,
            "platform_name": platform_name,
            "campaign_goal_type": campaign_goal_type,
            "post_volume": post_volume,
            "best_posting_time": best_posting_time,
            "creative_brief": creative_brief,
            "posts": posts,
            "status": "generated",
            "created_at": datetime.utcnow().isoformat()
        }
    )

    return build_response(200, {
        "message": "Cammi generated your campaign content",
        "data": {
            "campaign_name": campaign_name,
            "total_posts": total_posts,
            "posts": posts
        }
    })


def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }
