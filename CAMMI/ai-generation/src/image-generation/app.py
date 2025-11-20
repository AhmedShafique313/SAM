import json, base64, boto3,os
from google.oauth2.service_account import Credentials
from google import genai
from boto3.dynamodb.conditions import Key

# Initialize Secrets Manager client
secrets_client = boto3.client("secretsmanager")

def get_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"]) if "SecretString" in response else None

def normalize_private_key(pk: str) -> str:
    if pk is None:
        return None
    service_account_info = get_secret(secret_name)
    if not service_account_info:
        raise Exception(f"Failed to load secret: {secret_name}")

    # Normalize private key formatting
    service_account_info["private_key"] = normalize_private_key(
        service_account_info.get("private_key")
    )

    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    creds = Credentials.from_service_account_info(service_account_info, scopes=scopes)

    return genai.Client(
        vertexai=True,
        project=service_account_info["project_id"],
        location="us-central1",
        credentials=creds,
    )

# Initialize DynamoDB
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")

def get_user_by_session(session_id: str):
    response = users_table.query(
        IndexName="session_id-index",
        KeyConditionExpression=Key("session_id").eq(session_id)
    )
    if not response["Items"]:
        return None
    return response["Items"][0]

def update_user_credits(email: str, new_credits: int):
    users_table.update_item(
        Key={"email": email},
        UpdateExpression="SET total_credits = :val",
        ExpressionAttributeValues={":val": new_credits}
    )
    
def is_unsafe_prompt(prompt: str) -> bool:
    prompt = prompt.lower()
    adult_keywords = [
        "nude", "naked", "sex", "sexual", "porn", "erotic", "nsfw", "boobs",
        "breasts", "genitals", "vagina", "penis", "nipple", "fetish", "explicit",
        "lingerie", "underwear", "xxx", "adult", "sensual", "provocative"
    ]
    return any(word in prompt for word in adult_keywords)

def lambda_handler(event, context):
    body = json.loads(event.get("body", "{}"))
    session_id = body.get("session_id")
    prompt = body.get("prompt", "")

    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }

    if not session_id:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({"error": "Missing session_id"})
        }

    user = get_user_by_session(session_id)
    if not user:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": "Unknown User"
        }

    email = user["email"]
    total_credits = int(user.get("total_credits", 0))

    if is_unsafe_prompt(prompt):
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({
                "session_id": session_id,
                "error": "Prompt contains restricted or unsafe content"
            })
        }

    if total_credits < 2:
        return {
            "statusCode": 402,
            "headers": cors_headers,
            "body": json.dumps({
                "session_id": session_id,
                "error": "Insufficient credits",
                "total_credits": total_credits
            })
        }

    client = build_client()

    # -------- accept up to 2 images (jpg/jpeg/png) via JSON base64 --------
    images = body.get("images", []) or []
    if len(images) > 2:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({"error": "You can upload at most 2 images"})
        }

    allowed_mimes = {"image/jpeg", "image/jpg", "image/png"}
    image_parts = []
    for idx, img in enumerate(images):
        mime = (img.get("mime_type") or "").lower()
        data_b64 = img.get("data")
        if mime not in allowed_mimes:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Image {idx+1} must be jpg/jpeg/png"})
            }
        try:
            cleaned = (data_b64 or "").strip()
            if cleaned.startswith("data:"):
                cleaned = cleaned.split(",", 1)[1]
            cleaned = cleaned.replace("\n", "").replace("\r", "").replace(" ", "")
            img_bytes = base64.b64decode(cleaned, validate=True)
        except Exception:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Image {idx+1} is not valid base64"})
            }
        # (Light validation of magic bytes)
        if mime in {"image/jpeg", "image/jpg"} and not img_bytes.startswith(b"\xff\xd8"):
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Image {idx+1} content is not a valid JPEG"})
            }
        if mime == "image/png" and not img_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Image {idx+1} content is not a valid PNG"})
            }
        image_parts.append({"inline_data": {"mime_type": mime, "data": img_bytes}})
    # ------------------------------------------------------------------------

    # Call model
    result = client.models.generate_content(
        model="publishers/google/models/gemini-2.5-flash-image",
        contents=[{
            "role": "user",
            "parts": [{"text": prompt}] + image_parts  # text first, then up to 2 images
        }]
    )

    # ---- SAFE EXTRACTION: only charge credits if we actually got an image ----
    candidate = (getattr(result, "candidates", None) or [None])[0]
    if not candidate or not getattr(candidate, "content", None) or not getattr(candidate.content, "parts", None):
        return {
            "statusCode": 502,
            "headers": cors_headers,
            "body": json.dumps({"error": "Model returned no content"})
        }

    image_bytes = None
    for p in candidate.content.parts:
        if getattr(p, "inline_data", None) and getattr(p.inline_data, "data", None):
            image_bytes = p.inline_data.data
            break

    if not image_bytes:
        return {
            "statusCode": 502,
            "headers": cors_headers,
            "body": json.dumps({"error": "Model did not return an image"})
        }
    # -------------------------------------------------------------------------

    # Deduct credits ONLY AFTER success
    new_credits = max(total_credits - 2, 0)
    update_user_credits(email, new_credits)

    image_base64 = base64.b64encode(image_bytes).decode("utf-8")

    response = {
        "session_id": session_id,
        "remaining_credits": new_credits,
        "image_base64": image_base64
    }

    return {
        "statusCode": 200,
        "headers": cors_headers,
        # "headers": {"Content-Type": "application/json"},
        "body": json.dumps(response)
    }

