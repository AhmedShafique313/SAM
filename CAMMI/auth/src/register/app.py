import json
import os
import uuid
import boto3
import hashlib
import hmac
import random
import time
from datetime import datetime
 
# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")  # Allow all origins
SES_SENDER = os.environ.get("SES_SENDER", "info@cammi.ai")  # must be verified in SES
VERIFY_TTL_MIN = int(os.environ.get("VERIFY_TTL_MIN", "10"))  # minutes
 
# ---------- AWS Clients ----------
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(USERS_TABLE)
ses = boto3.client("ses")
 
# ---------- Helpers ----------
def hash_password(password: str) -> str:
    salt = os.urandom(16)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode('utf-8'), salt, 100000)
    return salt.hex() + ":" + hashed.hex()
 
def verify_password(stored_password: str, provided_password: str) -> bool:
    try:
        salt_hex, hashed_hex = stored_password.split(":")
        new_hash = hashlib.pbkdf2_hmac('sha256', provided_password.encode('utf-8'), bytes.fromhex(salt_hex), 100000)
        return hmac.compare_digest(hashed_hex, new_hash.hex())
    except Exception:
        return False
 
def random_code() -> str:
    return str(random.SystemRandom().randint(100000, 999999))
 
def hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()

def has_local_password(user: dict) -> bool:
    return bool((user or {}).get("password"))

def merge_auth_providers(user: dict, provider: str):
    providers = user.get("auth_providers") if isinstance(user, dict) else None
    if not isinstance(providers, list):
        providers = []
    if provider not in providers:
        providers.append(provider)
    return providers
 
def send_email_code(to_email: str, code: str, purpose: str = "Email Verification"):
    subject = f"{purpose} Code"
    body = f"Your {purpose.lower()} code is: {code}\nThis code will expire in {VERIFY_TTL_MIN} minutes."
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [to_email]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body}}
        }
    )
 
def base_headers(event):
    origin = (event.get("headers") or {}).get("origin", FRONTEND_ORIGIN)
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",  # needed if frontend sends cookies
        "Access-Control-Allow-Headers": "*",  # allow all headers
        "Access-Control-Allow-Methods": "*",  # allow all HTTP methods
        "Access-Control-Max-Age": "3600"  # preflight cache for 1 hour
    }
 
def resp(status, headers, body_dict):
    return {
        "statusCode": status,
        "headers": headers,
        "body": json.dumps(body_dict)
    }
 
# ---------- Lambda ----------
def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path = event.get("path", "")
    headers = base_headers(event)
 
    # CORS preflight
    if method == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": json.dumps({"message": "CORS ok"})}
 
    # --------- /register ---------
    if path == "/auth/register" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            password = body.get("password")
            first_name = (body.get("firstName") or "").strip()
            last_name = (body.get("lastName") or "").strip()
 
            if not email or not password or not first_name or not last_name:
                return resp(400, headers, {"message": "Email, password, first name, and last name are required"})
 
            existing = table.get_item(Key={"email": email}).get("Item")
 
            if existing and existing.get("status") == "ACTIVE" and has_local_password(existing):
                return resp(409, headers, {"message": "User already exists and is active"})
 
            code = random_code()
            code_hash = hash_code(code)
            expires_at = int(time.time()) + VERIFY_TTL_MIN * 60
            hashed_password = hash_password(password)
 
            if existing and existing.get("status") == "PENDING":
                table.update_item(
                    Key={"email": email},
                    UpdateExpression=(
                        "SET #pwd = :p, #status = :s, verification_code_hash = :h, "
                        "verification_expires_at = :e, firstName = :fn, lastName = :ln, "
                        "auth_providers = :ap, pending_password_link = :pl"
                    ),
                    ExpressionAttributeNames={"#pwd": "password", "#status": "status"},
                    ExpressionAttributeValues={
                        ":p": hashed_password,
                        ":s": "PENDING",
                        ":h": code_hash,
                        ":e": expires_at,
                        ":fn": first_name,
                        ":ln": last_name,
                        ":ap": merge_auth_providers(existing, "password"),
                        ":pl": False
                    }
                )
            elif existing and not has_local_password(existing):
                # Existing account (for example Google sign-in) with no local password.
                # Require OTP before linking a new password to this same account.
                table.update_item(
                    Key={"email": email},
                    UpdateExpression=(
                        "SET pending_password_hash = :p, pending_password_link = :pl, "
                        "verification_code_hash = :h, verification_expires_at = :e, "
                        "firstName = if_not_exists(firstName, :fn), "
                        "lastName = if_not_exists(lastName, :ln)"
                    ),
                    ExpressionAttributeValues={
                        ":p": hashed_password,
                        ":pl": True,
                        ":h": code_hash,
                        ":e": expires_at,
                        ":fn": first_name,
                        ":ln": last_name,
                    }
                )
            elif existing:
                # Existing local account in a non-standard state: refresh verification
                # data in place instead of creating a new identity.
                table.update_item(
                    Key={"email": email},
                    UpdateExpression=(
                        "SET #pwd = :p, #status = :s, verification_code_hash = :h, "
                        "verification_expires_at = :e, firstName = :fn, lastName = :ln, "
                        "auth_providers = :ap"
                    ),
                    ExpressionAttributeNames={"#pwd": "password", "#status": "status"},
                    ExpressionAttributeValues={
                        ":p": hashed_password,
                        ":s": "PENDING",
                        ":h": code_hash,
                        ":e": expires_at,
                        ":fn": first_name,
                        ":ln": last_name,
                        ":ap": merge_auth_providers(existing, "password"),
                    }
                )
            else:
                user = {
                    "email": email,
                    "id": str(uuid.uuid4()),
                    "password": hashed_password,
                    "firstName": first_name,
                    "lastName": last_name,
                    "status": "PENDING",
                    "verification_code_hash": code_hash,
                    "verification_expires_at": expires_at,
                    "email_verified": False,
                    "auth_providers": ["password"],
                    "pending_password_link": False,
                    "onboarding_status": True,
                    "createdAt": datetime.utcnow().isoformat(),
                    "total_credits": 250,
                    "dashboard_status": False,
                    "chat_status": False,
                    "chatfact_status": False,
                    "breakdown_status": False,
                    "generation_status": False,
                    "generation_completed_status": False,
                    "submit_for_review_status": False,
                    "connector_status": False,
                    "add_profile_status": False,
                    "campaign_start_status": False,
                    "campaign_goal_status": False,
                    "campaign_recommend_status": False,
                    "campaign_posts_status": False,
                    "posts_history_status": False,
                    "campaign_dashboard_status": False,
                    "quick_posts_status": False,
                    "cammi_assistant_status": False,
                    "image_generation_status": False
                }
                table.put_item(Item=user)
 
            send_email_code(email, code, purpose="Email Verification")
 
            return resp(200, headers, {
                "message": "Verification code sent to email. Complete verification to activate your account."
            })
 
        except Exception as e:
            print("Register error:", str(e))
            return resp(500, headers, {"message": "Server error", "error": str(e)})
 
    # --------- /verify-email ---------
    if path == "/auth/verify-email" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            code = (body.get("code") or "").strip()
 
            if not email or not code:
                return resp(400, headers, {"message": "Email and code are required"})
 
            user = table.get_item(Key={"email": email}).get("Item")
            is_pending_signup = bool(user and user.get("status") == "PENDING")
            is_password_link = bool(user and user.get("pending_password_link"))

            if not user or (not is_pending_signup and not is_password_link):
                return resp(400, headers, {"message": "Invalid request or user not pending"})
 
            expected_hash = user.get("verification_code_hash")
            expires_at = int(user.get("verification_expires_at", 0))
 
            if not expected_hash or time.time() > expires_at:
                return resp(400, headers, {"message": "Code expired. Please register again to receive a new code."})
 
            if not hmac.compare_digest(expected_hash, hash_code(code)):
                return resp(401, headers, {"message": "Invalid verification code"})
 
            auth_providers = merge_auth_providers(user, "password")

            if is_password_link:
                pending_password_hash = user.get("pending_password_hash")
                if not pending_password_hash:
                    return resp(400, headers, {"message": "No pending password to link. Please register again."})

                table.update_item(
                    Key={"email": email},
                    UpdateExpression=(
                        "SET #status = if_not_exists(#status, :a), "
                        "session_id = if_not_exists(session_id, :sid), "
                        "password = :p, auth_providers = :ap, email_verified = :ev "
                        "REMOVE verification_code_hash, verification_expires_at, "
                        "pending_password_hash, pending_password_link"
                    ),
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":a": "ACTIVE",
                        ":sid": "NOT_LOGGED_IN",
                        ":p": pending_password_hash,
                        ":ap": auth_providers,
                        ":ev": True,
                    }
                )
                success_message = "Email verified and password linked to your existing account."
            else:
                table.update_item(
                    Key={"email": email},
                    UpdateExpression=(
                        "SET #status = :a, "
                        "session_id = if_not_exists(session_id, :sid), "
                        "auth_providers = :ap, email_verified = :ev "
                        "REMOVE verification_code_hash, verification_expires_at"
                    ),
                    ExpressionAttributeNames={"#status": "status"},
                    ExpressionAttributeValues={
                        ":a": "ACTIVE",
                        ":sid": "NOT_LOGGED_IN",  # empty until login
                        ":ap": auth_providers,
                        ":ev": True,
                    }
                )
                success_message = "Email verified. Your account is active."
 
            return resp(200, headers, {
                "message": success_message,
                "user": {
                    "email": user["email"],
                    "id": user["id"],
                    "firstName": user.get("firstName", ""),
                    "lastName": user.get("lastName", ""),
                    "dashboard_status": False,
                    "chat_status": False,
                    "chatfact_status": False,
                    "breakdown_status": False,
                    "generation_status": False,
                    "generation_completed_status": False,
                    "submit_for_review_status": False,
                    "connector_status": False,
                    "add_profile_status": False,
                    "campaign_start_status": False,
                    "campaign_goal_status": False,
                    "campaign_recommend_status": False,
                    "campaign_posts_status": False,
                    "posts_history_status": False,
                    "campaign_dashboard_status": False,
                    "quick_posts_status": False,
                    "cammi_assistant_status": False,
                    "image_generation_status": False
                }
            })
 
        except Exception as e:
            print("Verify email error:", str(e))
            return resp(500, headers, {"message": "Server error", "error": str(e)})
 
    # Fallback
    return resp(404, headers, {"message": "Not found"})
