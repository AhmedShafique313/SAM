import json
import base64
import time
import signal
import boto3
import os

from google.oauth2.service_account import Credentials
from google import genai
from google.genai import types
from boto3.dynamodb.conditions import Key


# --------------------------------------------------------
# HARD TIMEOUT HANDLER
# --------------------------------------------------------
def timeout_handler(signum, frame):
    raise TimeoutError("Model call exceeded time limit")

signal.signal(signal.SIGALRM, timeout_handler)


# --------------------------------------------------------
# LOAD GOOGLE SERVICE ACCOUNT FROM SECRETS MANAGER
# --------------------------------------------------------
secrets_client = boto3.client("secretsmanager")
secret_name = os.environ.get("GCP_SERVICE_SECRET_NAME")

secret_value = secrets_client.get_secret_value(SecretId=secret_name)["SecretString"]
google_sa_json = json.loads(secret_value)


def normalize_private_key(pk: str):
    if not pk:
        return pk
    pk = pk.strip()
    if (pk.startswith('"') and pk.endswith('"')) or (pk.startswith("'") and pk.endswith("'")):
        pk = pk[1:-1]
    return pk.replace("\\n", "\n")


def build_client():
    google_sa_json["private_key"] = normalize_private_key(google_sa_json["private_key"])

    creds = Credentials.from_service_account_info(
        google_sa_json,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )

    return genai.Client(
        vertexai=True,
        project=google_sa_json["project_id"],
        location="us-central1",
        credentials=creds,
    )


client = build_client()


# --------------------------------------------------------
# DYNAMODB
# --------------------------------------------------------
dynamodb = boto3.resource("dynamodb")
users_table_name = os.environ.get("USERS_TABLE")
users_table = dynamodb.Table(users_table_name)


def get_user_by_session(session_id):
    res = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id),
    )
    return res["Items"][0] if res["Items"] else None


def update_user_credits(email, amount):
    users_table.update_item(
        Key={"email": email},
        UpdateExpression="SET total_credits = :v",
        ExpressionAttributeValues={":v": amount},
    )


# --------------------------------------------------------
# SAFETY CHECKS
# --------------------------------------------------------
def is_unsafe_prompt(t: str):
    banned = [
        "nude", "naked", "sex", "sexual", "porn", "erotic", "nsfw",
        "vagina", "penis", "genitals", "fetish", "xxx",
        "lingerie", "underwear"
    ]
    return any(x in t.lower() for x in banned)


# --------------------------------------------------------
# GEMINI 2.5 FLASH IMAGE
# --------------------------------------------------------
def call_gemini(prompt: str, images_raw, mime_types):

    parts = [types.Part(text=prompt)]

    for i in range(len(images_raw)):
        parts.append(
            types.Part(
                inline_data=types.Blob(
                    mime_type=mime_types[i],
                    data=images_raw[i],
                )
            )
        )

    user_content = types.Content(role="user", parts=parts)

    return client.models.generate_content(
        model="publishers/google/models/gemini-2.5-flash-image",
        contents=[user_content],
    )


# --------------------------------------------------------
# IMAGEN 4 FALLBACK
# --------------------------------------------------------
def call_imagen_fallback(text_prompt: str):
    return client.models.generate_images(
        model="publishers/google/models/imagen-4.0-ultra-generate-001",
        prompt=text_prompt
    )


# --------------------------------------------------------
# MAIN HANDLER (UNCHANGED)
# --------------------------------------------------------
def lambda_handler(event, context):

    cors = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }

    try:
        body = json.loads(event.get("body", "{}"))
    except:
        return {"statusCode": 400, "headers": cors,
                "body": json.dumps({"error": "Invalid JSON"})}

    session_id = body.get("session_id")
    raw_user_prompt = (body.get("prompt") or "").strip()
    images = body.get("images", []) or []

    if not session_id:
        return {"statusCode": 400, "headers": cors,
                "body": json.dumps({"error": "Missing session_id"})}

    user = get_user_by_session(session_id)
    if not user:
        return {"statusCode": 400, "headers": cors,
                "body": json.dumps({"error": "Unknown user"})}

    email = user["email"]
    credits = int(user.get("total_credits", 0))

    if credits < 2:
        return {"statusCode": 402, "headers": cors,
                "body": json.dumps({"error": "Insufficient credits"})}

    if raw_user_prompt and is_unsafe_prompt(raw_user_prompt):
        return {"statusCode": 400, "headers": cors,
                "body": json.dumps({"error": "Unsafe prompt"})}

    if len(images) > 2:
        return {"statusCode": 400, "headers": cors,
                "body": json.dumps({"error": "Max 2 images allowed"})}

    images_raw = []
    mime_types = []

    for idx, img in enumerate(images):
        mime = (img.get("mime_type") or "").lower()

        if mime not in ["image/png", "image/jpeg", "image/jpg"]:
            return {"statusCode": 400, "headers": cors,
                    "body": json.dumps({"error": f"Image {idx+1} invalid format"})}

        data_b64 = img.get("data", "")
        if data_b64.startswith("data:"):
            data_b64 = data_b64.split(",", 1)[1]

        try:
            raw_bytes = base64.b64decode(data_b64, validate=True)
        except:
            return {"statusCode": 400, "headers": cors,
                    "body": json.dumps({"error": f"Image {idx+1} invalid base64"})}

        images_raw.append(raw_bytes)
        mime_types.append(mime)

    if images_raw:
        prompt = f"""
Use the uploaded image(s) as the MAIN visual reference.
Do NOT ignore them.

User request: {raw_user_prompt or "Enhance the uploaded image."}
"""
    else:
        prompt = f"""
Generate an image based on the user prompt.

User request: {raw_user_prompt or "Abstract artistic image."}
"""

    try:
        signal.alarm(16)
        gemini_result = call_gemini(prompt, images_raw, mime_types)
        signal.alarm(0)
    except Exception:
        gemini_result = None
        signal.alarm(0)

    if gemini_result:
        try:
            image_bytes = None
            cand = gemini_result.candidates[0]

            for p in cand.content.parts:
                if getattr(p, "inline_data", None):
                    image_bytes = p.inline_data.data
                    break

            if image_bytes:
                output_b64 = base64.b64encode(image_bytes).decode()
                new_credits = max(credits - 2, 0)
                update_user_credits(email, new_credits)

                return {
                    "statusCode": 200,
                    "headers": cors,
                    "body": json.dumps({
                        "session_id": session_id,
                        "remaining_credits": new_credits,
                        "image_base64": output_b64
                    })
                }

        except:
            pass

    fallback_prompt = raw_user_prompt if raw_user_prompt else "Abstract artistic image."

    try:
        signal.alarm(12)
        imagen_result = call_imagen_fallback(fallback_prompt)
        signal.alarm(0)
    except Exception as e:
        signal.alarm(0)
        return {"statusCode": 502, "headers": cors,
                "body": json.dumps({"error": "Fallback model failed", "details": str(e)})}

    try:
        image_bytes = imagen_result.images[0].image_bytes
    except:
        return {"statusCode": 502, "headers": cors,
                "body": json.dumps({"error": "Imagen fallback returned no image"})}

    output_b64 = base64.b64encode(image_bytes).decode()

    new_credits = max(credits - 2, 0)
    update_user_credits(email, new_credits)

    return {
        "statusCode": 200,
        "headers": cors,
        "body": json.dumps({
            "session_id": session_id,
            "remaining_credits": new_credits,
            "image_base64": output_b64
        })
    }
