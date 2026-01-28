import json
import boto3
import uuid
import math
from boto3.dynamodb.conditions import Key
from datetime import datetime, timedelta, timezone
from botocore.config import Config
# ---------- AWS clients ----------
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    config=Config(connect_timeout=60, read_timeout=300)
)
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")
posts_table = dynamodb.Table("posts-table")
DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
# ---------- Helper ----------
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
# ---------- Scheduling helper ----------
def calculate_scheduled_time(
    campaign_start: datetime,
    campaign_duration_days: int,
    best_post_day: str,
    best_post_time: str,
    post_index: int,
    total_posts: int
) -> str:
    weekday_map = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6
    }
    # Parse time (fallback 09:00)
    hour, minute = 9, 0
    try:
        t = datetime.strptime(best_post_time.strip(), "%I:%M %p")
        hour, minute = t.hour, t.minute
    except Exception:
        pass
    spacing_days = max(1, campaign_duration_days // max(1, total_posts))
    base_date = campaign_start + timedelta(days=post_index * spacing_days)
    target_weekday = weekday_map.get(
        best_post_day.lower(), base_date.weekday()
    )
    days_ahead = (target_weekday - base_date.weekday()) % 7
    scheduled_date = base_date + timedelta(days=days_ahead)
    scheduled_datetime = scheduled_date.replace(
        hour=hour,
        minute=minute,
        second=0,
        microsecond=0
    )
    # ✅ ONLY CHANGE: add UTC offset
    return scheduled_datetime.replace(tzinfo=timezone.utc).isoformat()
# ---------- Lambda handler ----------
def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")
    project_id = body.get("project_id")
    campaign_id = body.get("campaign_id")
    if not all([session_id, project_id, campaign_id]):
        return build_response(400, {"error": "Missing required fields"})
    # ---------- Get user ----------
    user_resp = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )
    if not user_resp.get("Items"):
        return build_response(404, {"error": "User not found"})
    user_id = user_resp["Items"][0]["id"]
    # ---------- Get campaign ----------
    campaign_resp = campaigns_table.get_item(
        Key={"campaign_id": campaign_id, "project_id": project_id}
    )
    if "Item" not in campaign_resp:
        return build_response(404, {"error": "Campaign not found"})
    campaign = campaign_resp["Item"]
    campaign_name = campaign.get("campaign_name") or "Untitled Campaign"
    platform_name = campaign.get("platform_name")
    creative_brief = campaign.get("creative_brief")
    key_message = campaign.get("key_message")
    total_posts = int(campaign.get("total_posts", 0))
    posts_per_week = campaign.get("posts_per_week")
    campaign_goal_type = campaign.get("campaign_goal_type")
    campaign_duration_days = int(campaign.get("campaign_duration_days", 0))
    brand_tone = campaign.get("brand_tone")
    brand_voice = campaign.get("brand_voice")
    if total_posts <= 0:
        return build_response(400, {"error": "Invalid total_posts value"})
    # ---------- Scheduling setup ----------
    campaign_start_time = datetime.utcnow()
    post_counter = 0
    # ---------- Batch processing ----------
    batch_size = 5
    batches = math.ceil(total_posts / batch_size)
    all_posts = []
    for i in range(batches):
        current_batch = min(batch_size, total_posts - i * batch_size)
        prompt = f"""
You are a senior social media strategist.
Generate EXACTLY {current_batch} posts.
STRICT RULES:
- Output MUST start with {{ and end with }}
- No explanations
- No markdown
- No text before or after JSON
JSON FORMAT:
{{
  "posts": [
    {{"title": string, "description": string, "caption": string, "hashtags": [string],
      "keywords": [string], "image_generation_prompt": string, "best_post_time": string,
      "best_post_day": string}}
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
        try:
            llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)
            batch_posts = json.loads(llm_response).get("posts", [])
        except json.JSONDecodeError:
            return build_response(500, {"error": f"Invalid JSON from LLM in batch {i+1}"})
        for post in batch_posts:
            post_id = uuid.uuid4().hex[:12]
            scheduled_time = calculate_scheduled_time(
                campaign_start=campaign_start_time,
                campaign_duration_days=campaign_duration_days,
                best_post_day=post.get("best_post_day", "Monday"),
                best_post_time=post.get("best_post_time", "09:00 AM"),
                post_index=post_counter,
                total_posts=total_posts
            )
            post_counter += 1
            posts_table.update_item(
                Key={"post_id": post_id, "campaign_id": campaign_id},
                UpdateExpression="""
                    SET
                        title = :title,
                        description = :description,
                        hashtags = :hashtags,
                        image_generation_prompt = :image_generation_prompt,
                        best_post_time = :best_post_time,
                        best_post_day = :best_post_day,
                        scheduled_time = :scheduled_time,
                        generated_at = :generated_at,
                        #s = :status
                """,
                ExpressionAttributeNames={
                    "#s": "status"   # best practice (status is a common reserved word)
                },
                ExpressionAttributeValues={
                    ":title": post.get("title"),
                    ":description": post.get("description"),
                    ":hashtags": post.get("hashtags"),
                    ":image_generation_prompt": post.get("image_generation_prompt"),
                    ":best_post_time": post.get("best_post_time"),
                    ":best_post_day": post.get("best_post_day"),
                    ":scheduled_time": scheduled_time,
                    ":generated_at": datetime.utcnow().isoformat(),
                    ":status": "Generated"
                }
            )
            all_posts.append({
                "post_id": post_id,
                "title": post.get("title"),
                "description": post.get("description"),
                "hashtags": post.get("hashtags"),
                "image_generation_prompt": post.get("image_generation_prompt"),
                "best_post_time": post.get("best_post_time"),
                "best_post_day": post.get("best_post_day"),
                "scheduled_time": scheduled_time
            })
    # ---------- Update campaign status ----------
    campaigns_table.update_item(
        Key={"campaign_id": campaign_id, "project_id": project_id},
        UpdateExpression="SET #s = :status",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":status": "Generated"}
    )
    return build_response(200, {
        "message": "Cammi generated your campaign content",
        "campaign_name": campaign_name,
        "campaign_goal_type": campaign_goal_type,
        "data": {
            "status": "Generated",
            "total_posts": total_posts,
            "posts": all_posts
        }
    })
# ---------- Response helper ----------
def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json"
            # CORS is handled by Lambda Function URL - no headers needed here
        },
        "body": json.dumps(body)
    }