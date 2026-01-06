import json
import boto3

# Initialize Bedrock Runtime client
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

# Claude 4 Sonnet model ID
MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"


def invoke_claude(prompt: str) -> dict:
    system_prompt = """
You are an expert SEO strategist and social media expert.

Your task:
- Analyze the user's idea or content prompt
- Generate (max 5 each):
  1. SEO-friendly keywords (primary, secondary)
  2. Relevant social media hashtags related to the user prompt

Strict rules:
- Output ONLY valid JSON
- No markdown
- No explanations
- No additional text
- Keywords: short, high-intent phrases
- Hashtags: lowercase, no spaces

Required JSON schema:
{
  "keywords": ["keyword1", "keyword2"],
  "hashtags": ["#hashtag1", "#hashtag2"]
}
"""

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": f"{system_prompt}\n\nUser Input:\n{prompt}"
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

    # Claude always returns text → parse it as JSON
    response_text = response["output"]["message"]["content"][0]["text"]
    return json.loads(response_text)


def lambda_handler(event, context):
    body = event.get("body")

    # Body must be a string
    if not body or not isinstance(body, str):
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": "Request body must be a JSON string"})
        }

    body_json = json.loads(body)
    prompt = body_json.get("prompt")

    # Prompt must be a string
    if not prompt or not isinstance(prompt, str):
        return {
            "statusCode": 400,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": json.dumps({"error": "Field 'prompt' must be a string"})
        }

    claude_output = invoke_claude(prompt.strip())

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps(claude_output)
    }
