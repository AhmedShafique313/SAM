import json
from groq import Groq
import os
secrets_client = boto3.client("secretsmanager")
 
def get_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"]) if "SecretString" in response else None
 
client = get_secret(os.environ["GROQ_API_KEY"])

def invoke_groq(prompt: str):
    instruction = (
        """You are a senior business strategist. 
Your task is to check the user’s response and decide if you should refine it, guide them, or give direct suggestions.

THINKING STEP:

Read the question and the user’s answer together.
If the answer is relevant or has something useful, refine it into a strong, clean business statement.
If the answer is vague, irrelevant, or confused, give guidance in a soft, simple, suggestion tone.
If the user directly asks for ideas, names, or examples, give the actual suggestions, not guidance.

DECISION LOGIC:
IF the user’s answer is relevant:
Refine it into a clear, professional business statement.
Output only that statement but increase its word count such that the information becomes more rich context wise but do not generate any facts or stats by yourself no matter what.
IF the user’s answer is vague or unclear:
Give short, easy-to-follow guidance in a suggestion tone.
Use very simple words, but keep the tone business professional.
Give at least 2 easy examples to show how other businesses might answer.
IF the user directly asks for ideas or help:
Give direct answers or lists of ideas.
Do not give guidance or explanations.
If the user provides any link or URL just output that URL as is.

OUTPUT RULES:
- Output must always be in plain text.
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

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt.strip()},
        ],
    )

    return response.choices[0].message.content.strip()

def lambda_handler(event, context):
    if not event.get("body"):
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Empty body received"}),
        }

    body = json.loads(event["body"])
    prompt = body.get("prompt", "").strip()
    # session_id = body.get("session_id", "")

    if not prompt:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Missing 'prompt' in request body"}),
        }

    groq_response = invoke_groq(prompt)

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json",
        },
        "body": json.dumps(
            {
                "message": "Response generated successfully",
                # "session_id": session_id,
                "groq_response": groq_response,
            }
        ),
    }
    
