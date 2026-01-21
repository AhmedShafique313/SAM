import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")
BUCKET_NAME = "cammi-devprod"
DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")

def llm_calling(prompt: str, model_id: str) -> str:
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
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")
    campaign_goal_type = body.get("campaign_goal_type")
    platform_name = body.get("platform_name")  # optional
    brand_tone_input = body.get("brand_tone")  # optional (not used in overwrite)

    if not all([session_id, project_id, campaign_id, campaign_goal_type]):
        return build_response(400, {"error": "Missing required fields"})

    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user = user_resp["Items"][0]
    user_id = user["id"]

    users_table.update_item(
        Key={"email": user["email"]},
        UpdateExpression="ADD campaigns :c",
        ExpressionAttributeValues={
            ":c": {campaign_id}
        }
    )

    s3_key = f"knowledgebase/{user_id}/{user_id}_campaign_data.txt"
    s3_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
    campaign_context = s3_obj["Body"].read().decode("utf-8")

    prompt = f"""
You are a senior paid media and brand strategist.

Campaign Goal Type:
{campaign_goal_type}

Primary Platform: LinkedIn

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
  "post_volume": {{
    "total_posts": number,
    "posts_per_week": number
  }},
  "best_posting_time": string,
  "creative_brief": string
}}
"""

    llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)
    generated_campaign = json.loads(llm_response)
    campaign_type_obj = generated_campaign.get("campaign_type", {})
    campaign_duration_days = generated_campaign.get("campaign_duration_days")
    best_suited_platform = generated_campaign.get("best_suited_platform")

    brand_tone = campaign_type_obj.get("brand_tone")
    brand_voice = campaign_type_obj.get("brand_voice")
    key_message = campaign_type_obj.get("key_message")

    post_volume_obj = generated_campaign.get("post_volume", {})
    total_posts = post_volume_obj.get("total_posts")
    posts_per_week = post_volume_obj.get("posts_per_week")

    best_posting_time = generated_campaign.get("best_posting_time")
    creative_brief = generated_campaign.get("creative_brief")

    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,   # Partition Key
            "project_id": project_id      # Sort Key
        },
        UpdateExpression="""
            SET
                user_id = :user_id,
                campaign_goal_type = :campaign_goal_type,
                platform_name = :platform_name,
                campaign_duration_days = :campaign_duration_days,
                best_suited_platform = :best_suited_platform,
                brand_tone = :brand_tone,
                brand_voice = :brand_voice,
                key_message = :key_message,
                total_posts = :total_posts,
                posts_per_week = :posts_per_week,
                best_posting_time = :best_posting_time,
                creative_brief = :creative_brief,
                updated_at = :updated_at
        """,
        ExpressionAttributeValues={
            ":user_id": user_id,
            ":campaign_goal_type": campaign_goal_type,
            ":platform_name": best_suited_platform,
            ":campaign_duration_days": campaign_duration_days,
            ":best_suited_platform": best_suited_platform,
            ":brand_tone": brand_tone,
            ":brand_voice": brand_voice,
            ":key_message": key_message,
            ":total_posts": total_posts,
            ":posts_per_week": posts_per_week,
            ":best_posting_time": best_posting_time,
            ":creative_brief": creative_brief,
            ":updated_at": datetime.utcnow().isoformat()
        }
    )


    return build_response(
        200,
        {
            "message": "Cammi is analyzing your input",
            "data": generated_campaign
        }
    )


def build_response(status: int, body: dict):
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
