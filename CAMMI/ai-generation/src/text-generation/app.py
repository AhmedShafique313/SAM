import json
from groq import Groq
import os
import boto3
from boto3.dynamodb.conditions import Attr

secrets_client = boto3.client("secretsmanager")
def get_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"]) if "SecretString" in response else None

client = get_secret(os.environ["GROQ_API_KEY"])

# ✅ Function that handles all business logic
def get_concatenated_post_answers(organization_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('PostQuestions')

    # ✅ Scan all records where organization_id matches
    all_items = []
    response = table.scan(
        FilterExpression=Attr('organization_id').eq(organization_id)
    )
    all_items.extend(response.get('Items', []))

    # ✅ Handle pagination (if dataset >1MB)
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Attr('organization_id').eq(organization_id),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        all_items.extend(response.get('Items', []))

    # ✅ Concatenate all post_answer values
    all_answers = '\n'.join(
        item.get('post_answer', '') for item in all_items if item.get('post_answer')
    ).strip()

    return all_answers


 
def invoke_groq(prompt: str,answers: str):
    instruction = (
        f"""
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

Immediately after this heading, output a two-column text table exactly like this format (no headers or dividers):

| Objective | [goal] |
| KPI | [goal_specific_KPI] |
| Campaign Theme | [campaign_theme] |
| CTA | [CTA] |
| Hashtags | [industry_hashtags] |
| UTM Link | [landing_URL_with_channel_substitution] |

2) Next heading:

Carousel Slides

Then produce another two-column table (no headers, no dividers):

| S1 Title | [hero_message] |
| S2 Title | Insight addressing [pain_points] |
| S3 Title | Benefit proof from [proof_points] |
| S4 Title | Result or transformation relevant to [role] in [industry] |
| S5 Title | How CAMMI helps teams of [company_size] move faster |
| S6 Title | [CTA] |
| Alt Text | Describe slides using [tone] and [visual_refs] |

3) Heading:

Post Copy:

Then write one paragraph (120–180 words) for the LinkedIn post caption.
Keep tone professional, benefits-focused, and clear.
Incorporate [role], [company_size], [industry], and [pain_points].
Mention [hero_message] early in the copy and at least one data point from [proof_points].
End with [CTA] and [landing_URL_with_channel_substitution].
No lists, hashtags, or emojis.

4) Heading:

Post 2 — Instagram Carousel

Then output another two-column text table (no headers, no dividers):

| Objective | [goal] |
| Primary KPI | [goal_specific_KPI] |
| Hashtags | [industry_hashtags] |
| CTA | [CTA] |

5) Heading:

Slides & Alt Text

Then another two-column table (no headers, no dividers):

| S1 | [hero_message] |
| S2 | [pain_points_short_phrase] |
| S3 | [proof_points] |
| S4 | Result / emotional benefit aligned with [goal] |
| S5 | [CTA] |
| Alt | Visual description using [tone] and [visual_refs] |

6) Heading:

Caption:

Then write one paragraph (120–150 words) for the Instagram post caption.
Keep tone conversational but still professional ([tone]).
Speak directly to [role]s in [industry] companies of [company_size].
Address [pain_points], include [proof_points], reinforce [campaign_theme], and close with [CTA] + [landing_URL_with_channel_substitution].
No lists, hashtags, or emojis.

Rules:

All tables must render with | vertical bars and two columns only.

Do not include markdown formatting (no backticks, headers, or separators).

Do not add any explanation or preamble — begin output directly with the line:
Post 1 — LinkedIn Carousel

Maintain the section order and headings exactly as specified.

All content must align with [campaign_theme].
Below you'll find the context.
  
"""
    )
 
    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt.strip()},
        ],
    )
    
    return response.choices[0].message.content.strip()

def finalized_result(post_prompt: str):
    instructions = (
        """

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
    )
    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": post_prompt.strip()},
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
    groq_response = invoke_groq(prompt,answers)
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