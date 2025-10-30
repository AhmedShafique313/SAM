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
    if path == "/register" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            password = body.get("password")
            first_name = (body.get("firstName") or "").strip()
            last_name = (body.get("lastName") or "").strip()
 
            if not email or not password or not first_name or not last_name:
                return resp(400, headers, {"message": "Email, password, first name, and last name are required"})
 
            existing = table.get_item(Key={"email": email}).get("Item")
 
            if existing and existing.get("status") == "ACTIVE":
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
                        "verification_expires_at = :e, firstName = :fn, lastName = :ln"
                    ),
                    ExpressionAttributeNames={"#pwd": "password", "#status": "status"},
                    ExpressionAttributeValues={
                        ":p": hashed_password,
                        ":s": "PENDING",
                        ":h": code_hash,
                        ":e": expires_at,
                        ":fn": first_name,
                        ":ln": last_name
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
                    "onboarding_status": True,
                    "createdAt": datetime.utcnow().isoformat(),
                    "total_credits": 250
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
    if path == "/verify-email" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            code = (body.get("code") or "").strip()
 
            if not email or not code:
                return resp(400, headers, {"message": "Email and code are required"})
 
            user = table.get_item(Key={"email": email}).get("Item")
            if not user or user.get("status") != "PENDING":
                return resp(400, headers, {"message": "Invalid request or user not pending"})
 
            expected_hash = user.get("verification_code_hash")
            expires_at = int(user.get("verification_expires_at", 0))
 
            if not expected_hash or time.time() > expires_at:
                return resp(400, headers, {"message": "Code expired. Please register again to receive a new code."})
 
            if not hmac.compare_digest(expected_hash, hash_code(code)):
                return resp(401, headers, {"message": "Invalid verification code"})
 
            table.update_item(
                Key={"email": email},
                UpdateExpression=(
                    "SET #status = :a, session_id = :sid "
                    "REMOVE verification_code_hash, verification_expires_at"
                ),
                ExpressionAttributeNames={"#status": "status"},
                ExpressionAttributeValues={
                    ":a": "ACTIVE",
                    ":sid": "NOT_LOGGED_IN"  # empty until login
                }
            )
 
            return resp(200, headers, {
                "message": "Email verified. Your account is active.",
                "user": {
                    "email": user["email"],
                    "id": user["id"],
                    "firstName": user.get("firstName", ""),
                    "lastName": user.get("lastName", "")
                }
            })
 
        except Exception as e:
            print("Verify email error:", str(e))
            return resp(500, headers, {"message": "Server error", "error": str(e)})
 
    # Fallback
    return resp(404, headers, {"message": "Not found"})
