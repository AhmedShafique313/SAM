import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from groq import Groq

# ✅ Initialize Groq client using the environment variable
client = Groq(api_key=os.environ["GROQ_API_KEY"])

# ✅ DynamoDB resource (reuse across invocations)
dynamodb = boto3.resource("dynamodb")
POST_QUESTIONS_TABLE = dynamodb.Table("post-questions-table")
GSI_NAME = "organization_id-index"  # Name of the GSI

# Helper: standard API response
def build_response(status_code, body_dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json",
        },
        "body": json.dumps(body_dict),
    }

# ✅ Fetch and concatenate all post_answers for organization_id using GSI
def get_concatenated_post_answers(organization_id):
    all_answers = []

    # Query using the GSI
    response = POST_QUESTIONS_TABLE.query(
        IndexName=GSI_NAME,
        KeyConditionExpression=Key("organization_id").eq(organization_id),
        ProjectionExpression="post_answer"
    )
    items = response.get("Items", [])
    all_answers.extend(item["post_answer"] for item in items if item.get("post_answer"))

    # Handle pagination
    while "LastEvaluatedKey" in response:
        response = POST_QUESTIONS_TABLE.query(
            IndexName=GSI_NAME,
            KeyConditionExpression=Key("organization_id").eq(organization_id),
            ProjectionExpression="post_answer",
            ExclusiveStartKey=response["LastEvaluatedKey"]
        )
        items = response.get("Items", [])
        all_answers.extend(item["post_answer"] for item in items if item.get("post_answer"))

    return "\n".join(all_answers).strip()


# ✅ Call Groq model to generate posts
def invoke_groq(prompt: str, answers: str):
    instruction = f"""
This is contextual information for reference and for user tailored experience.
{answers}
You are a B2B social strategist for CAMMI — an AI marketing copilot that helps B2B teams plan with clarity, align work, mobilize campaigns faster, and prove ROI with unified dashboards.

Inputs you can use:
[goal], [role], [company_size], [industry], [pain_points], [campaign_theme], [hero_message], [proof_points], [landing_URL], [CTA], [tone], [visual_refs].

Your task:
Generate LinkedIn and Instagram carousel posts using the exact structure and wording style of the approved M3_Social Media Post template.
Output must strictly follow the same formatting and order below.
All tables must be real text tables using vertical bars (|) and two columns only — no headers, no dividers, and no extra commentary before or after.

1) Start with this heading:

Post 1 — LinkedIn Carousel
...
"""  # Keep the full instructions as in your original code

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt},
        ],
    )
    return response.choices[0].message.content.strip()


# ✅ Finalize single LinkedIn post
def finalized_result(post_prompt: str):
    instructions = """
You are a senior B2B social strategist for CAMMI — an AI marketing copilot that helps B2B teams plan with clarity, align work, mobilize campaigns faster, and prove ROI with unified dashboards.

You have been provided with complete campaign information above (including objectives, KPIs, slides, captions, and CTAs).
Using all of that as context, generate a single complete LinkedIn post optimized for organic performance.

Your goals:
- Analyze the campaign details to understand the target audience, pain points, theme, hero message, and proof points.
- Write one LinkedIn post (150–200 words) that:
  - Opens with a strong, scroll-stopping insight or question.
  - Speaks directly to the ICP (e.g., Heads of Marketing at B2B SaaS companies).
  - Clearly expresses the campaign theme (“Run Marketing Like a Product”).
  - Highlights 1–2 proof points (e.g., 99% on-time launches, 3× faster activation).
  - Ends with a natural, confident call-to-action (e.g., “Book a Demo”).
  - Uses a professional, insight-driven, executive tone.

At the end of the post:
- Add exactly 5 relevant hashtags that align with the content’s theme, audience, and proof points.
- Include the correct campaign UTM link provided in the context.

**Output Requirements:**
- Do NOT include any section headers or labels like “LinkedIn Post Title”, “LinkedIn Post Copy”, “Hashtags”, or “UTM Link”.
- The output should be a single, polished LinkedIn post — clean, well-formatted, and ready to publish directly on LinkedIn.
"""

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": post_prompt},
        ],
    )
    return response.choices[0].message.content.strip()


# ✅ Lambda Handler (no try/except)
def lambda_handler(event, context):
    if not event.get("body"):
        return build_response(400, {"error": "Empty body received"})

    body = json.loads(event["body"])
    prompt = body.get("prompt", "").strip()
    organization_id = body.get("organization_id", "").strip()

    if not prompt:
        return build_response(400, {"error": "Missing 'prompt' in request body"})

    # Fetch answers from DynamoDB using GSI
    answers = get_concatenated_post_answers(organization_id)
    print("Concatenated answers length:", len(answers))

    # Generate Groq response
    groq_response = invoke_groq(prompt, answers)
    final_response = finalized_result(groq_response)

    return build_response(
        200,
        {
            "message": "Response generated successfully",
            "final_response": final_response,
        },
    )
