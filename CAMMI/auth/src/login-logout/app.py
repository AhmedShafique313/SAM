import json
import os
import uuid
import boto3
import hashlib
import hmac
 
# ---------- Config ----------
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
FRONTEND_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")  # * allows all origins
 
# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(USERS_TABLE)
 
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
 
            # Capture the original onboarding_status before update
            original_onboarding_status = user.get("onboarding_status", False)
 
            # Always update session_id
            update_expr = "SET session_id = :s"
            expr_attr_values = {":s": session_id}
 
            # If onboarding_status was True, set it to False in DB
            if original_onboarding_status is True:
                update_expr += ", onboarding_status = :o"
                expr_attr_values[":o"] = False
 
            table.update_item(
                Key={"email": email},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_attr_values,
            )

            #Combine firstName + lastName for full name
            full_name = f"{user.get('firstName', '').strip()} {user.get('lastName', '').strip()}".strip()
 
            return resp(
                200,
                headers,
                {
                    "message": "Login successful",
                    "token": session_id,
                    # Return the original value before we updated
                    "onboarding_status": original_onboarding_status,
                    "user": {
                        "email": user["email"],
                        "id": user["id"],
                        "name": full_name,
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
