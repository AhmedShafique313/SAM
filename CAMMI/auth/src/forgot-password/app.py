import json
import os
import boto3
import hashlib
import hmac
import random
import time
from datetime import datetime

# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")
SES_SENDER = os.environ.get("SES_SENDER", "info@cammi.ai")
VERIFY_TTL_MIN = int(os.environ.get("VERIFY_TTL_MIN", "10"))  # minutes

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
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
        new_hash = hashlib.pbkdf2_hmac(
            'sha256', provided_password.encode('utf-8'), bytes.fromhex(salt_hex), 100000
        )
        return hmac.compare_digest(hashed_hex, new_hash.hex())
    except Exception:
        return False

def random_code() -> str:
    return str(random.SystemRandom().randint(100000, 999999))

def hash_code(code: str) -> str:
    return hashlib.sha256(code.encode("utf-8")).hexdigest()

def send_email_code(to_email: str, code: str, purpose: str = "Password Reset"):
    subject = f"{purpose} Code"
    body = (
        f"Your {purpose.lower()} code is: {code}\n\n"
        f"This code will expire in {VERIFY_TTL_MIN} minutes.\n"
        "If you did not request this, please ignore this email."
    )
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
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Max-Age": "3600",
        "Content-Type": "application/json"
    }

def resp(status, event, body_dict):
    return {
        "statusCode": status,
        "headers": base_headers(event),
        "body": json.dumps(body_dict)
    }

# ---------- Lambda ----------
def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path = event.get("path", "")

    # CORS preflight
    if method == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": base_headers(event),
            "body": json.dumps({"message": "CORS preflight OK"})
        }

    # --------- /forgot-password ---------
    if path == "/auth/forgot-password" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            if not email:
                return resp(400, event, {"message": "Email is required"})

            user_resp = table.get_item(Key={"email": email})
            user = user_resp.get("Item")
            if not user:
                return resp(200, event, {"message": "If an account with that email exists, a reset code has been sent."})

            code = random_code()
            code_hash = hash_code(code)
            expires_at = int(time.time()) + VERIFY_TTL_MIN * 60

            table.update_item(
                Key={"email": email},
                UpdateExpression="SET verification_code_hash = :h, verification_expires_at = :e",
                ExpressionAttributeValues={":h": code_hash, ":e": expires_at}
            )

            send_email_code(email, code, purpose="Password Reset")

            return resp(200, event, {"message": "If an account with that email exists, a reset code has been sent."})

        except Exception as e:
            print("Forgot password error:", str(e))
            return resp(500, event, {"message": "Server error", "error": str(e)})

    # --------- /verify-code ---------
    if path == "/auth/verify-code" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            code = (body.get("code") or "").strip()

            if not email or not code:
                return resp(400, event, {"message": "Email and code are required"})

            user_resp = table.get_item(Key={"email": email})
            user = user_resp.get("Item")
            if not user:
                return resp(400, event, {"message": "Invalid request"})

            expected_hash = user.get("verification_code_hash")
            expires_at = int(user.get("verification_expires_at", 0))

            if not expected_hash or time.time() > expires_at:
                return resp(400, event, {"message": "Code expired or invalid. Please request a new code."})

            if not hmac.compare_digest(expected_hash, hash_code(code)):
                return resp(401, event, {"message": "Invalid verification code"})

            # ✅ Mark as verified (optional: store a flag)
            table.update_item(
                Key={"email": email},
                UpdateExpression="SET code_verified = :v",
                ExpressionAttributeValues={":v": True}
            )

            return resp(200, event, {"message": "Code verified successfully"})

        except Exception as e:
            print("Verify code error:", str(e))
            return resp(500, event, {"message": "Server error", "error": str(e)})

  # --------- /reset-password ---------
    if path == "/auth/reset-password" and method == "POST":
        try:
            body = json.loads(event.get("body", "{}"))
            email = (body.get("email") or "").strip().lower()
            new_password = body.get("newPassword")
            confirm_password = body.get("confirmPassword")

            if not email or not new_password or not confirm_password:
                return resp(400, event, {"message": "Email, newPassword and confirmPassword are required"})

            if new_password != confirm_password:
                return resp(400, event, {"message": "newPassword and confirmPassword do not match"})

            user_resp = table.get_item(Key={"email": email})
            user = user_resp.get("Item")
            if not user:
                return resp(400, event, {"message": "Invalid request"})

            # ✅ Ensure code was verified first
            if not user.get("code_verified"):
                return resp(400, event, {"message": "Code not verified. Please verify first."})

            hashed_password = hash_password(new_password)

            table.update_item(
                Key={"email": email},
                UpdateExpression=(
                    "SET #pwd = :p REMOVE verification_code_hash, verification_expires_at, code_verified"
                ),
                ExpressionAttributeNames={"#pwd": "password"},
                ExpressionAttributeValues={":p": hashed_password}
            )

            return resp(200, event, {"message": "Password reset successful. You may now login with your new password."})

        except Exception as e:
            print("Reset password error:", str(e))
            return resp(500, event, {"message": "Server error", "error": str(e)})
               
    # Fallback
    return resp(404, event, {"message": "Not found"})
