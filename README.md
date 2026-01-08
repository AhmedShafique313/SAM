SAM/
│
├── .github/
│   └── workflows/
│       └── deploy.yml               # CI/CD automation for SAM build & deploy
│
└── CAMMI/
    ├── template.yaml                # 🧩 Root SAM orchestrator (nested stacks)
    ├── samconfig.toml               # Build/deploy configuration
    │
    ├── layers/
    │   ├── template.yaml            # Google libraries layer definition
    │   └── layer_google.zip         # Packaged dependencies
    │
    ├── db/
    │   └── template.yaml            # ✅ Contains users & feedback DynamoDB tables
    │
    ├── auth/
    │   ├── template.yaml            # Google OAuth Lambda definition
    │   └── src/
    │       └── continue-with-google.py
    │
    ├── API/
    │   └── template.yaml            # API Gateway resources & methods
    │
    └── feedback/
        ├── template.yaml            # Customer feedback Lambdas (2 functions)
        └── src/
            ├── customer-feedback.py
            └── check-customer-feedback.py



### AI - Suggestion Code:
# Agentic DAG
import json
import boto3
from datetime import datetime
from langchain_community.tools import DuckDuckGoSearchRun

ddg_search = DuckDuckGoSearchRun()
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

def run_research(current_time_iso: str, platform: str, media_type: str) -> str:
    dt = datetime.fromisoformat(current_time_iso)

    day_name = dt.strftime("%A")          
    month_year = dt.strftime("%B %Y")     

    queries = [
        f"Best time to post on {platform} {day_name} {month_year}",
        f"{platform} algorithm updates {month_year} for {media_type}",
        f"Current trending topics on {platform} {day_name} {month_year}"
    ]

    results = []
    for query in queries:
        search_result = ddg_search.run(query)
        results.append(f"QUERY: {query}\nRESULT:\n{search_result}")
    return "\n\n".join(results)


def invoke_claude_agent(current_time_iso: str, platform: str, media_type: str, research_data: str) -> dict:
    system_prompt = """
You are an expert social media strategist operating as an autonomous agent.

You have access to recent (2026) research data.

Your task:
- Analyze the provided research data
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

Research Data:
{research_data}
"""

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": system_prompt + "\n\n" + user_prompt
                }
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
    body = event.get("body")
    if not body or not isinstance(body, str):
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": "Request body must be a JSON string"})
        }
    payload = json.loads(body)
    current_time = payload.get("current_time")
    platform = payload.get("platform", "linkedin").lower()
    media_type = payload.get("media_type", "post").lower()
    if not current_time or not isinstance(current_time, str):
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": "Field 'current_time' must be an ISO 8601 string"})
        }
    
    research_data = run_research(
        current_time_iso=current_time,
        platform=platform,
        media_type=media_type
    )

    ai_output = invoke_claude_agent(
        current_time_iso=current_time,
        platform=platform,
        media_type=media_type,
        research_data=research_data
    )

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(ai_output)
    }