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
You are an expert LinkedIn content strategist and a senior B2B social strategist for CAMMI — an AI marketing copilot that helps B2B teams plan with clarity, align work, mobilize campaigns faster, and prove ROI with unified dashboards.
 
⚠️ CRITICAL CONTEXT
- You have been provided with contextual campaign information above, generated from user answers.
- Your output will be published directly to LinkedIn as-is, without any human editing.
- You must be fact-safe, format-safe, and assumption-safe at all times.
 
Your first step (internal):
- Analyze the provided context and the user's refined input to determine the topic.
- Identify whether the topic is:
  a) B2B / business / anything business or marketing related  
  b) Non-business (e.g., cars, sports, entertainment, general interest)
 
━━━━━━━━━━━━━━━━━━
CONTENT SAFETY RULES (STRICT)
━━━━━━━━━━━━━━━━━━
 
You MUST NOT invent or fabricate:
- Events, venues, dates, tickets, or live experiences
- Slides, carousels, or multi-frame formats unless explicitly provided
- Technical specifications, metrics, performance numbers, or statistics unless explicitly present in the context
 
If information is missing or unclear:
- Generalize instead of fabricating
- Use qualitative or comparative language (e.g., “known for performance”, “designed for scale”, “engineered for efficiency”)
 
━━━━━━━━━━━━━━━━━━
WRITING RULES (NON-NEGOTIABLE)
━━━━━━━━━━━━━━━━━━
 
- Generate ONE single LinkedIn post only
- Length: 120-180 words
- Start with a strong hook or scroll-stopping question
- Tone must be:
  - Credible
  - Insight-driven
  - Engaging
  - Natural and professional
- Encourage discussion rather than selling unless a CTA is explicitly appropriate
 
━━━━━━━━━━━━━━━━━━
TOPIC-SPECIFIC BEHAVIOR
━━━━━━━━━━━━━━━━━━
 
IF the topic is B2B / Marketing / business:
- Speak directly to the relevant ICP when identifiable (e.g., Heads of Marketing, B2B SaaS leaders)
- Clearly communicate the campaign theme if provided
- Highlight 1-2 proof points ONLY if they exist in the context
- End with a natural, confident CTA (e.g., “Book a demo”, “Learn more”)
- Include the campaign UTM link ONLY if it exists in the context
 
IF the topic is NON-BUSINESS (cars, sports, entertainment, fandom):
- DO NOT add CTAs such as “Book now”, “Register”, “Buy”, or “Sign up”
- Focus on insight, comparison, perspective, or discussion
- Keep it conversational and curiosity-driven
- Do NOT include links unless explicitly provided
 
━━━━━━━━━━━━━━━━━━
OUTPUT RULES (ABSOLUTE)
━━━━━━━━━━━━━━━━━━
 
- Output ONLY the final LinkedIn post
- No headings, labels, explanations, or meta commentary
- No placeholders (no [date], [link], [company])
- No slide references
- Add EXACTLY 5 relevant hashtags at the end
- The post must be immediately safe and ready for LinkedIn publishing
 
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
 