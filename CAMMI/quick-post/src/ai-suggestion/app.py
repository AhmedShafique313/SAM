import json
import boto3
from datetime import datetime

bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


def invoke_claude_agent(current_time_iso: str, platform: str, media_type: str) -> dict:
    """
    Calls Claude via Bedrock to get recommended posting times.
    """
    system_prompt = """
You are an expert social media strategist operating as an autonomous agent.

Your task:
- Generate 30-minute posting slots for the next 24 hours
- Score each slot based on:
  1. Algorithm alignment
  2. Audience receptivity
- Adjust recommendations based on the current ISO time provided
- Select the SINGLE best posting time

Strict rules:
- Output ONLY valid JSON
- No markdown
- No explanations outside JSON
- Use ISO 8601 datetime format

Required JSON schema:
{
  "recommended_post_time": "YYYY-MM-DDTHH:MM:SS",
  "platform": "linkedin",
  "media_type": "post",
  "reasoning": "concise, data-grounded explanation"
}
"""

    user_prompt = f"""
Current Time (ISO): {current_time_iso}
Platform: {platform}
Media Type: {media_type}
"""

    messages = [
        {
            "role": "user",
            "content": [
                {"text": system_prompt + "\n\n" + user_prompt}
            ]
        }
    ]

    response = bedrock_runtime.converse(
        modelId=MODEL_ID,
        messages=messages,
        inferenceConfig={
            "temperature": 0.6,
            "topP": 0.9
        }
    )

    response_text = response["output"]["message"]["content"][0]["text"]
    return json.loads(response_text)


def lambda_handler(event, context):
    """
    AWS Lambda handler
    """

    # ✅ Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": _cors_headers(),
            "body": ""
        }

    body = event.get("body")
    if not body or not isinstance(body, str):
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Request body must be a JSON string"})
        }

    payload = json.loads(body)
    current_time = payload.get("current_time")
    platform = payload.get("platform", "linkedin").lower()
    media_type = payload.get("media_type", "post").lower()

    if not current_time or not isinstance(current_time, str):
        return {
            "statusCode": 400,
            "headers": _cors_headers(),
            "body": json.dumps({"error": "Field 'current_time' must be an ISO 8601 string"})
        }

    ai_output = invoke_claude_agent(
        current_time_iso=current_time,
        platform=platform,
        media_type=media_type
    )

    return {
        "statusCode": 200,
        "headers": _cors_headers(),
        "body": json.dumps(ai_output)
    }


def _cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }
