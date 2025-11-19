import json
import boto3
from botocore.exceptions import ClientError

# AWS Bedrock client
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# ---------- Claude (Bedrock) Call ----------
def invoke_claude(prompt: str, session_id: str, model_id: str) -> str:
    instruction = (
        """You are a senior business strategist. 
Your task is to check the user's response and decide if you should refine it, guide them, or give direct suggestions.

THINKING STEP:

Read the question and the user's answer together.
If the answer is relevant or has something useful, refine it into a strong, clean business statement.
If the answer is vague, irrelevant, or confused, give guidance in a soft, simple, suggestion tone.
If the user directly asks for ideas, names, or examples, give the actual suggestions, not guidance.

DECISION LOGIC:
IF the user's answer is relevant:
Refine it into a clear, professional business statement.
Output only that statement but increase its word count such that the information becomes more rich context wise but do not generate any facts or stats by yourself no matter what.
IF the user's answer is vague or unclear:
Give short, easy-to-follow guidance in a suggestion tone.
Use very simple words, but keep the tone business professional.
Give at least 2 easy examples to show how other businesses might answer.
IF the user directly asks for ideas or help:
Give direct answers or lists of ideas.
Do not give guidance or explanations.
If the user provides any link or URL just output that URL as is.

OUTPUT RULES:
- Output must always be in plain text and must not be more than 110 words.
- Always sound business professional, but explain in very simple, easy words
- Never mix guidance and refinement in one output
- Refinement: only the polished business statement
- Guidance: clear, simple suggestions with examples
- Direct ask: give the answer straight
- Keep the language clean, smooth, and easy to understand
- No extra questions, No long lectures
- Never use Markdown formatting like *, **, _, or #.
- Do not use bullet symbols that depend on formatting. Use plain text only.
- Use capitalization, spacing, or line breaks everywhere where emphasis is needed."""
    )

    # Combine instruction and user prompt
    full_prompt = f"{instruction}\n\nUser Input: {prompt}"

    conversation = [
        {
            "role": "user",
            "content": [{"text": full_prompt}]
        }
    ]

    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=conversation,
        inferenceConfig={
            "maxTokens": 60000,
            "temperature": 0.7,
            "topP": 0.9
        },
        requestMetadata={
            "sessionId": session_id
        }
    )

    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text.strip()


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        # ✅ Validate request body
        if not event.get("body"):
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({"error": "Empty body received"})
            }

        body = json.loads(event["body"])
        prompt = body.get("prompt", "").strip()
        session_id = body.get("session_id", "default-session")
        model_id = body.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")

        if not prompt:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Content-Type": "application/json"
                },
                "body": json.dumps({"error": "Missing 'prompt' in request body"})
            }

        # ✅ Invoke Claude via Bedrock
        claude_response = invoke_claude(prompt, session_id, model_id)

        # ✅ SAME RESPONSE FORMAT AS ORIGINAL GROQ VERSION
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "message": "Response generated successfully",
                "groq_response": claude_response  # ← CHANGED: using "groq_response" for frontend compatibility
            })
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": f"AWS Bedrock Error: {str(e)}"
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json"
            },
            "body": json.dumps({
                "error": str(e)
            })
        }