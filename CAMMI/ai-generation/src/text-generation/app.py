import json
import os
import boto3
from boto3.dynamodb.conditions import Attr
from groq import Groq


# ✅ Initialize Groq client using the environment variable (already resolved from Secrets Manager)
client = Groq(api_key=os.environ["GROQ_API_KEY"])


# ✅ Function that handles all business logic
def get_concatenated_post_answers(organization_id):
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table("post-questions-table")

    # ✅ Scan all records where organization_id matches
    all_items = []
    response = table.scan(
        FilterExpression=Attr("organization_id").eq(organization_id)
    )
    all_items.extend(response.get("Items", []))

    # ✅ Handle pagination (if dataset >1MB)
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression=Attr("organization_id").eq(organization_id),
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        all_items.extend(response.get("Items", []))

    # ✅ Concatenate all post_answer values
    all_answers = "\n".join(
        item.get("post_answer", "") for item in all_items if item.get("post_answer")
    ).strip()

    return all_answers


# ✅ Call Groq model for post generation
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
"""

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt.strip()},
        ],
    )

    return response.choices[0].message.content.strip()


# ✅ Finalized post formatter
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
            {"role": "user", "content": post_prompt.strip()},
        ],
    )
    return response.choices[0].message.content.strip()


# ✅ Lambda Handler
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
    organization_id = body.get("organization_id", "")

    if not prompt:
        return {
            "statusCode": 400,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Content-Type": "application/json",
            },
            "body": json.dumps({"error": "Missing 'prompt' in request body"}),
        }

    answers = get_concatenated_post_answers(organization_id)
    print("these are concatenated answers", answers)
    groq_response = invoke_groq(prompt, answers)
    final_response = finalized_result(groq_response)

    return {
        "statusCode": 200,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json",
        },
        "body": json.dumps(
            {
                "message": "Response generated successfully",
                "final_response": final_response,
            }
        ),
    }
