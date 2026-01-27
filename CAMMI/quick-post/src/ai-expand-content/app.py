import json
import boto3

# Initialize Bedrock Runtime client
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

# Claude 4 Sonnet model ID
MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Common CORS headers
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",  # replace with your domain in production
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json"
}


def invoke_claude(prompt: str) -> dict:
    system_prompt = """
You are an expert social media copywriter and SEO strategist.

Your task:
- Analyze the user's idea or content prompt
- Generate ONE social media caption JSON containing:
  1. title: short, catchy post title
  2. description: engaging post description (1–3 sentences)
  3. hashtags: relevant hashtags, all lowercase, separated by a single space

Strict rules:
- Output ONLY valid JSON
- No markdown
- No explanations
- No additional text
- Hashtags must be lowercase and space-separated (NOT an array)

Required JSON schema:
{
  "title": "post title here",
  "description": "post description here",
  "hashtags": "#hashtag1 #hashtag2 #hashtag3"
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

    response_text = response["output"]["message"]["content"][0]["text"]
    return json.loads(response_text)


def lambda_handler(event, context):
    # Handle preflight OPTIONS request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": ""
        }

    body = event.get("body")

    # Body must be a string
    if not body or not isinstance(body, str):
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Request body must be a JSON string"})
        }

    try:
        body_json = json.loads(body)
    except json.JSONDecodeError:
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Invalid JSON in request body"})
        }

    prompt = body_json.get("prompt")

    # Prompt must be a string
    if not prompt or not isinstance(prompt, str):
        return {
            "statusCode": 400,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Field 'prompt' must be a string"})
        }

    try:
        claude_output = invoke_claude(prompt.strip())
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": "Model invocation failed", "details": str(e)})
        }

    # Wrap the Claude JSON in a top-level "caption"
    return {
        "statusCode": 200,
        "headers": CORS_HEADERS,
        "body": json.dumps({"caption": claude_output})
    }
