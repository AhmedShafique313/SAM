import json
import boto3
import uuid
from boto3.dynamodb.conditions import Key
from datetime import datetime

bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")
posts_table = dynamodb.Table("posts-table")

DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


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


def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))

    session_id = body.get("session_id")
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")

    if not session_id or not project_id or not campaign_id:
        return build_response(400, {"error": "Missing required fields"})

    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})

    user_id = user_resp["Items"][0]["id"]

    campaign_resp = campaigns_table.get_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        }
    )

    if "Item" not in campaign_resp:
        return build_response(404, {"error": "Campaign not found"})

    campaign = campaign_resp["Item"]

    platform_name = campaign.get("platform_name")
    creative_brief = campaign.get("creative_brief")
    key_message = campaign.get("key_message")
    total_posts = int(campaign.get("total_posts", 0))
    posts_per_week = campaign.get("posts_per_week")
    campaign_goal_type = campaign.get("campaign_goal_type")
    campaign_duration_days = campaign.get("campaign_duration_days")
    brand_tone = campaign.get("brand_tone")
    brand_voice = campaign.get("brand_voice")

    if total_posts <= 0:
        return build_response(400, {"error": "Invalid total_posts value"})

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
Campaign Duration (days): {campaign_duration_days}
Posts Per Week: {posts_per_week}
Brand Tone: {brand_tone}
Brand Voice: {brand_voice}
Key Message: {key_message}
Creative Brief: {creative_brief}
"""

    llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)

    try:
        generated = json.loads(llm_response)
    except json.JSONDecodeError:
        return build_response(500, {"error": "Invalid LLM JSON response"})

    posts_list = generated.get("posts", [])

    saved_posts = []

    for post in posts_list:
        post_id = uuid.uuid4().hex[:12]  # ✅ small unique UUID

        title = post.get("title")
        description = post.get("description")
        hashtags = post.get("hashtags")
        image_generation_prompt = post.get("image_generation_prompt")
        best_post_time = post.get("best_post_time")
        best_post_day = post.get("best_post_day")

        posts_table.update_item(
            Key={
                "post_id": post_id,          # ✅ Partition Key
                "campaign_id": campaign_id  # ✅ Sort Key
            },
            UpdateExpression="""
                SET
                    title = :title,
                    description = :description,
                    hashtags = :hashtags,
                    image_generation_prompt = :image_generation_prompt,
                    best_post_time = :best_post_time,
                    best_post_day = :best_post_day,
                    generated_at = :generated_at
            """,
            ExpressionAttributeValues={
                ":title": title,
                ":description": description,
                ":hashtags": hashtags,
                ":image_generation_prompt": image_generation_prompt,
                ":best_post_time": best_post_time,
                ":best_post_day": best_post_day,
                ":generated_at": datetime.utcnow().isoformat()
            }
        )

        saved_posts.append({
            "post_id": post_id,
            "title": title,
            "description": description,
            "hashtags": hashtags,
            "image_generation_prompt": image_generation_prompt,
            "best_post_time": best_post_time,
            "best_post_day": best_post_day
        })

    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        },
        UpdateExpression="SET #s = :status",
        ExpressionAttributeNames={
            "#s": "status"
        },
        ExpressionAttributeValues={
            ":status": "Generated"
        }
    )

    return build_response(200, {
        "message": "Cammi generated your campaign content",
        "data": {
            "status": "Generated",
            "total_posts": total_posts,
            "posts": saved_posts
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
