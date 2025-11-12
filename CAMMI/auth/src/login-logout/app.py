import json
import os
import uuid
import boto3
import hashlib
import hmac
from boto3.dynamodb.conditions import Key

 
# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")  # * allows all origins
# Count-based onboarding (no backend list of questions)
ONBOARDING_TABLE = os.environ.get("ONBOARDING_TABLE", "onboarding-questions-table")
TOTAL_REQUIRED_ONBOARDING_QUESTIONS = int(os.environ.get("TOTAL_REQUIRED_ONBOARDING_QUESTIONS", "7"))

 
# ---------- AWS Clients ----------
s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(USERS_TABLE)
onboarding_table = dynamodb.Table(ONBOARDING_TABLE)
 
# ---------- Helpers ----------
def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt_hex, hashed_hex = stored_password.split(":")
        new_hash = hashlib.pbkdf2_hmac(
            "sha256",
            provided_password.encode("utf-8"),
            bytes.fromhex(salt_hex),
            100000,
        )
        return hmac.compare_digest(hashed_hex, new_hash.hex())
    except Exception:
        return False
 
 
def base_headers(event):
    origin = (event.get("headers") or {}).get("origin", FRONTEND_ORIGIN)
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "*",
        "Access-Control-Max-Age": "3600",
    }
 
 
def resp(status, headers, body_dict):
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body_dict),
    }
 


def get_onboarding_answer_count(user_id: str) -> int:
    """
    Returns how many onboarding answers exist for this user_id
    by counting items in the Onboarding table (PK=user_id).
    Uses Select='COUNT' and paginates if needed.
    """
    total = 0
    last_evaluated_key = None
    while True:
        params = {
            "KeyConditionExpression": Key("user_id").eq(user_id),
            "Select": "COUNT",
        }
        if last_evaluated_key:
            params["ExclusiveStartKey"] = last_evaluated_key

        resp = onboarding_table.query(**params)
        total += resp.get("Count", 0)
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return total

 
# ---------- Lambda ----------
def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path = event.get("path", "")
    headers = base_headers(event)

    # CORS preflight
    if method == "OPTIONS":
        return resp(200, headers, {"message": "CORS ok"})

    # --------- /login ---------
    if path == "/login" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            password = body.get("password")

            if not email or not password:
                return resp(400, headers, {"message": "Email and password required"})

            user = table.get_item(Key={"email": email}).get("Item")
            if not user:
                return resp(404, headers, {"message": "User not found"})

            if user.get("status") != "ACTIVE":
                return resp(403, headers, {"message": "User not verified yet"})

            if not verify_password(user["password"], password):
                return resp(401, headers, {"message": "Invalid password"})

            session_id = str(uuid.uuid4())

            # ----- COUNT-BASED ONBOARDING CHECK -----
            # If user has fewer than TOTAL_REQUIRED_ONBOARDING_QUESTIONS answers in the
            # Onboarding table (partition key = user_id), keep onboarding_status = True.
            # Otherwise set to False.
            remaining_questions = None
            computed_onboarding_status = False
            try:
                user_id = user.get("id")
                if user_id:
                    answered = get_onboarding_answer_count(user_id)
                    remaining_questions = max(TOTAL_REQUIRED_ONBOARDING_QUESTIONS - answered, 0)
                    computed_onboarding_status = remaining_questions > 0
                else:
                    # If record has no id, be safe and require onboarding
                    computed_onboarding_status = True
                    remaining_questions = TOTAL_REQUIRED_ONBOARDING_QUESTIONS
            except Exception:
                # If counting fails, be safe and require onboarding
                computed_onboarding_status = True
                remaining_questions = TOTAL_REQUIRED_ONBOARDING_QUESTIONS

            # Persist fresh session_id + computed onboarding_status
            table.update_item(
                Key={"email": email},
                UpdateExpression="SET session_id = :s, onboarding_status = :o",
                ExpressionAttributeValues={
                    ":s": session_id,
                    ":o": computed_onboarding_status,
                },
            )

            # Combine firstName + lastName for full name
            full_name = f"{user.get('firstName', '').strip()} {user.get('lastName', '').strip()}".strip()

            raw_picture = (user.get("picture") or "").strip()
            picture_url = None
            if raw_picture:
                if raw_picture.startswith("https://lh3.googleusercontent.com"):
                    # Google avatar (letter or real photo)
                    picture_url = raw_picture
                elif ".s3.amazonaws.com/" in raw_picture:
                    # Convert S3 URL to a presigned URL (if object is private)
                    # Example raw_picture: https://cammi.s3.amazonaws.com/profile/<user_id>.jpeg
                    try:
                        # Extract bucket and key from the URL
                        # format: https://{bucket}.s3.amazonaws.com/{key...}   
                        without_scheme = raw_picture.split("://", 1)[-1]
                        host, key = without_scheme.split("/", 1)
                        bucket = host.split(".s3.amazonaws.com")[0]

                        picture_url = s3.generate_presigned_url(
                            ClientMethod="get_object",
                            Params={"Bucket": bucket, "Key": key},
                            ExpiresIn=60,  # 1 hour
                        )
                    except Exception as _e:
                        picture_url = raw_picture
                else:
                    picture_url = raw_picture
            return resp(
                200,
                headers,
                {
                    "message": "Login successful",
                    "token": session_id,
                    "onboarding_status": computed_onboarding_status,
                    "remaining_questions": remaining_questions,
                    "total_questions": TOTAL_REQUIRED_ONBOARDING_QUESTIONS,
                    "user": {
                        "email": user["email"],
                        "id": user["id"],
                        "name": full_name,
                        "picture": picture_url,
                    },
                },
            )

        except Exception as e:
            print("Login error:", str(e))
            return resp(
                500,
                headers,
                {"message": "Server error", "error": str(e)},
            )

    # --------- /logout ---------
    if path == "/logout" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            token = body.get("token")

            if not token:
                return resp(400, headers, {"message": "Token required"})

            # 🔍 Find user by scanning for session_id (since session_id is not a key)
            response = table.scan(
                FilterExpression="session_id = :s",
                ExpressionAttributeValues={":s": token},
            )

            items = response.get("Items", [])
            if not items:
                return resp(401, headers, {"message": "Invalid or expired token"})

            user = items[0]
            email = user["email"]

            # Remove session_id
            table.update_item(
                Key={"email": email},
                UpdateExpression="REMOVE session_id",
            )

            return resp(200, headers, {"message": "Logged out successfully"})

        except Exception as e:
            print("Logout error:", str(e))
            return resp(
                500,
                headers,
                {"message": "Server error", "error": str(e)},
            )

    # --------- Fallback ---------
    return resp(404, headers, {"message": "Not found"})
