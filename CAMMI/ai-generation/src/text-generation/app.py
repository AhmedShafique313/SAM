import json
import os
import boto3
from boto3.dynamodb.conditions import Key
from groq import Groq
from botocore.exceptions import ClientError

# ==============================
# ✅ INITIALIZATION
# ==============================

client = Groq(api_key=os.environ["GROQ_API_KEY"])

dynamodb = boto3.resource("dynamodb")

POST_QUESTIONS_TABLE = dynamodb.Table("post-questions-table")
USERS_TABLE = dynamodb.Table("users-table")

POST_GSI_NAME = "organization_id-index"
USER_GSI_NAME = "session_id-index"


# ==============================
# ✅ STANDARD RESPONSE BUILDER
# ==============================

def build_response(status_code, body_dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Content-Type": "application/json",
        },
        "body": json.dumps(body_dict),
    }


# ==============================
# ✅ FETCH POST ANSWERS
# ==============================

def get_concatenated_post_answers(organization_id):
    all_answers = []

    response = POST_QUESTIONS_TABLE.query(
        IndexName=POST_GSI_NAME,
        KeyConditionExpression=Key("organization_id").eq(organization_id),
        ProjectionExpression="post_answer",
    )

    items = response.get("Items", [])
    all_answers.extend(
        item["post_answer"] for item in items if item.get("post_answer")
    )

    while "LastEvaluatedKey" in response:
        response = POST_QUESTIONS_TABLE.query(
            IndexName=POST_GSI_NAME,
            KeyConditionExpression=Key("organization_id").eq(organization_id),
            ProjectionExpression="post_answer",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )

        items = response.get("Items", [])
        all_answers.extend(
            item["post_answer"] for item in items if item.get("post_answer")
        )

    return "\n".join(all_answers).strip()


# ==============================
# ✅ USER / CREDIT FUNCTIONS
# ==============================

def get_user_by_session(session_id):
    res = USERS_TABLE.query(
        IndexName=USER_GSI_NAME,
        KeyConditionExpression=Key("session_id").eq(session_id),
    )
    return res["Items"][0] if res.get("Items") else None


def deduct_credits_atomic(email, deduction):
    """
    Deduct credits safely using DynamoDB ConditionExpression.
    Prevents race conditions and negative credits.
    """
    try:
        USERS_TABLE.update_item(
            Key={"email": email},
            UpdateExpression="SET total_credits = total_credits - :d",
            ConditionExpression="total_credits >= :d",
            ExpressionAttributeValues={
                ":d": deduction,
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def get_user_credits(email):
    res = USERS_TABLE.get_item(Key={"email": email})
    user = res.get("Item", {})
    return int(user.get("total_credits", 0))


# ==============================
# ✅ GROQ GENERATION
# ==============================

def invoke_groq(prompt: str, answers: str):
    instruction = f"""
This is contextual information for reference and for user tailored experience.
{answers}

You are a B2B social strategist for CAMMI — an AI marketing copilot that helps B2B teams plan with clarity, align work, mobilize campaigns faster, and prove ROI with unified dashboards.

Generate LinkedIn and Instagram carousel posts using the approved structure.
"""

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instruction},
            {"role": "user", "content": prompt},
        ],
    )

    return response.choices[0].message.content.strip()


def finalized_result(post_prompt: str):
    instructions = """
You are an expert LinkedIn content strategist for CAMMI.

Generate ONE single LinkedIn post.
Length: 120-180 words.
Start with a strong hook.
Add EXACTLY 5 relevant hashtags.
Output only the final post.
"""

    response = client.chat.completions.create(
        model="openai/gpt-oss-20b",
        messages=[
            {"role": "system", "content": instructions},
            {"role": "user", "content": post_prompt},
        ],
    )

    return response.choices[0].message.content.strip()


# ==============================
# ✅ LAMBDA HANDLER
# ==============================

def lambda_handler(event, context):

    if not event.get("body"):
        return build_response(400, {"error": "Empty body received"})

    body = json.loads(event["body"])

    prompt = body.get("prompt", "").strip()
    organization_id = body.get("organization_id", "").strip()
    session_id = body.get("session_id", "").strip()

    if not prompt:
        return build_response(400, {"error": "Missing 'prompt'"})

    if not session_id:
        return build_response(401, {"error": "Missing session_id"})

    # ==============================
    # ✅ SESSION VALIDATION
    # ==============================

    user = get_user_by_session(session_id)

    if not user:
        return build_response(401, {"error": "Invalid session"})

    email = user.get("email")

    # ==============================
    # ✅ ATOMIC CREDIT DEDUCTION
    # ==============================

    success = deduct_credits_atomic(email, 2)

    if not success:
        current_credits = get_user_credits(email)
        return build_response(
            402,
            {
                "error": "Insufficient credits",
                "remaining_credits": current_credits,
            },
        )

    # Fetch updated credits after deduction
    new_credits = get_user_credits(email)

    # ==============================
    # ✅ FETCH CONTEXT DATA
    # ==============================

    answers = get_concatenated_post_answers(organization_id)

    # ==============================
    # ✅ GENERATE CONTENT
    # ==============================

    groq_response = invoke_groq(prompt, answers)
    final_response = finalized_result(groq_response)

    # ==============================
    # ✅ SUCCESS RESPONSE
    # ==============================

    return build_response(
        200,
        {
            "message": "Response generated successfully",
            "final_response": final_response,
            "remaining_credits": new_credits,
        },
    )