import json, uuid
import boto3, os
import requests
import cachecontrol
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
import google.auth.transport.requests
from urllib.parse import urlencode

CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
ZOHO_APP_PASSWORD = os.environ["ZOHO_APP_PASSWORD"]


REDIRECT_URI = "https://hobv2e2dna.execute-api.us-east-1.amazonaws.com/dev/auth/google-callback"
ZOHO_EMAIL = "info@cammi.ai"
USERS_TABLE = os.environ.get("USERS_TABLE", "users-table")
GOOGLE_AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URI = "https://oauth2.googleapis.com/token"
GOOGLE_SCOPES = [
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
]

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
}

dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table(USERS_TABLE)

def merge_auth_providers(user, provider):
    providers = user.get("auth_providers") if isinstance(user, dict) else None
    if not isinstance(providers, list):
        providers = []
    if provider not in providers:
        providers.append(provider)
    return providers

def split_name(full_name):
    parts = (full_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])

def build_google_flow(state=None):
    return Flow.from_client_config(
        {
            "web": {
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "auth_uri": GOOGLE_AUTH_URI,
                "token_uri": GOOGLE_TOKEN_URI,
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "redirect_uris": [REDIRECT_URI],
            }
        },
        scopes=GOOGLE_SCOPES,
        state=state,
        redirect_uri=REDIRECT_URI,
    )

def build_google_authorization_url():
    flow = build_google_flow()
    authorization_url, state = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )
    return authorization_url, state

# ------------------------
# Send welcome email
# ------------------------
def send_welcome_email(user_info):
    subject = "Welcome to CAMMI - Your AI-Powered Marketing Co-Pilot!"
    body_html = f"""
    <html>
      <body style="font-family: Arial, sans-serif; line-height:1.6; color:#333;">
        <h2>👋 Hello {user_info['name']},</h2>
        <p>Welcome to <b>CAMMI</b>! We're thrilled to have you on board.</p>
        <p>CAMMI simplifies your marketing workflow — strategy, content creation, scheduling, and tracking.</p>
        <h3>🚀 To get started:</h3>
        <ol>
          <li>Upload your brand collateral.</li>
          <li>Chat with CAMMI for strategies or content.</li>
          <li>Approve outputs before posting.</li>
        </ol>
        <p>💡 Need help? Email us at <a href="mailto:info@cammi.ai">info@cammi.ai</a></p>
        <p style="margin-top:20px;">Cheers,<br><b>The CAMMI Team</b></p>
      </body>
    </html>
    """
    msg = MIMEMultipart("alternative")
    msg["From"] = f"CAMMI Team <{ZOHO_EMAIL}>"
    msg["To"] = user_info["email"]
    msg["Subject"] = subject
    msg.attach(MIMEText("Welcome to CAMMI!", "plain"))
    msg.attach(MIMEText(body_html, "html"))

    with smtplib.SMTP_SSL("smtp.zoho.com", 465) as server:
        server.login(ZOHO_EMAIL, ZOHO_APP_PASSWORD)
        server.sendmail(ZOHO_EMAIL, user_info["email"], msg.as_string())

# ------------------------
# Login handler
# ------------------------
def login_lambda(event, context):
    try:
        authorization_url, state = build_google_authorization_url()
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "login_url": authorization_url,
                "state": state,
                "event": event
            })
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)})
        }

# ------------------------
# Callback handler
# ------------------------
def callback_lambda(event, context):
    try:
        query_params = event.get("queryStringParameters", {}) or {}
        if event.get("body"):
            try:
                body_data = json.loads(event["body"])
                query_params.update(body_data)
            except Exception:
                pass

        auth_code = query_params.get("code")
        if not auth_code:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "Missing authorization code", "event": event})
            }

        try:
            flow = build_google_flow(state=query_params.get("state"))
            flow.fetch_token(code=auth_code)
            credentials = flow.credentials
        except Exception as token_exc:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": f"Token exchange failed: {str(token_exc)}", "event": event})
            }

        access_token = credentials.token
        id_token_value = credentials.id_token
        expiry_value = str(credentials.expiry) if credentials.expiry else None

        if not id_token_value:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "Token response missing id_token", "event": event})
            }

        request_session = requests.session()
        cached_session = cachecontrol.CacheControl(request_session)
        token_request = google.auth.transport.requests.Request(session=cached_session)

        id_info = id_token.verify_oauth2_token(
            id_token=id_token_value,
            request=token_request,
            audience=CLIENT_ID,
        )

        session_id = str(uuid.uuid4())
        id = str(uuid.uuid4())

        normalized_email = (id_info.get("email") or "").strip().lower()
        first_name, last_name = split_name(id_info.get("name"))

        user_info = {
            "sub": id_info.get("sub"),
            "google_sub": id_info.get("sub"),
            "name": id_info.get("name"),
            "email": normalized_email,
            "firstName": first_name,
            "lastName": last_name,
            "picture": id_info.get("picture"),
            "locale": id_info.get("locale"),
            "access_token": access_token,
            "expiry": expiry_value,
            "session_id": session_id,
            "onboarding_status": True,
            "id": id,
            "status": "ACTIVE",
            "email_verified": True,
            "auth_providers": ["google"],
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

        frontend_onboarding_status = "true"

        existing_user = users_table.get_item(
            Key={"email": user_info["email"]}
        ).get("Item")

        if existing_user:
            id = existing_user.get("id") or str(uuid.uuid4())

            existing_first = (existing_user.get("firstName") or "").strip()
            existing_last = (existing_user.get("lastName") or "").strip()
            existing_full_name = f"{existing_first} {existing_last}".strip()
            final_name = existing_user.get("name") or existing_full_name or user_info["name"]
            auth_providers = merge_auth_providers(existing_user, "google")

            # ✅ FETCH STATUS FLAGS FROM DB
            dashboard_status = existing_user.get("dashboard_status", False)
            chat_status = existing_user.get("chat_status", False)
            chatfact_status = existing_user.get("chatfact_status", False)
            breakdown_status = existing_user.get("breakdown_status", False)
            generation_status = existing_user.get("generation_status", False)
            generation_completed_status = existing_user.get("generation_completed_status", False)
            submit_for_review_status = existing_user.get("submit_for_review_status", False)
            connector_status = existing_user.get("connector_status", False)
            add_profile_status = existing_user.get("add_profile_status", False)
            campaign_start_status = existing_user.get("campaign_start_status", False)
            campaign_goal_status = existing_user.get("campaign_goal_status", False)
            campaign_recommend_status = existing_user.get("campaign_recommend_status", False)
            campaign_posts_status = existing_user.get("campaign_posts_status", False)
            posts_history_status = existing_user.get("posts_history_status", False)
            campaign_dashboard_status = existing_user.get("campaign_dashboard_status", False)
            quick_posts_status = existing_user.get("quick_posts_status", False)
            cammi_assistant_status = existing_user.get("cammi_assistant_status", False)
            image_generation_status = existing_user.get("image_generation_status", False)

            users_table.update_item(
                Key={"email": user_info["email"]},
                UpdateExpression=(
                    "SET session_id = :session_id, id = :id, onboarding_status = :status, "
                    "#status = :active, email_verified = :verified, auth_providers = :providers, "
                    "#sub = :sub, google_sub = :google_sub, picture = :picture, locale = :locale, "
                    "access_token = :access_token, expiry = :expiry, "
                    "firstName = if_not_exists(firstName, :first_name), "
                    "lastName = if_not_exists(lastName, :last_name), "
                    "#name = if_not_exists(#name, :name)"
                ),
                ExpressionAttributeNames={"#name": "name", "#status": "status", "#sub": "sub"},
                ExpressionAttributeValues={
                    ":session_id": session_id,
                    ":id": id,
                    ":status": False,
                    ":active": "ACTIVE",
                    ":verified": True,
                    ":providers": auth_providers,
                    ":sub": id_info.get("sub"),
                    ":google_sub": id_info.get("sub"),
                    ":picture": id_info.get("picture"),
                    ":locale": id_info.get("locale"),
                    ":access_token": access_token,
                    ":expiry": expiry_value,
                    ":first_name": first_name,
                    ":last_name": last_name,
                    ":name": user_info["name"],
                }
            )

            frontend_onboarding_status = "false"

        else:
            id = str(uuid.uuid4())
            user_info["id"] = id
            users_table.put_item(Item=user_info)
            send_welcome_email(user_info)

            final_name = user_info["name"]
            dashboard_status = False
            chat_status = False
            chatfact_status = False
            breakdown_status = False
            generation_status = False
            generation_completed_status = False
            submit_for_review_status = False
            connector_status = False
            add_profile_status = False
            campaign_start_status = False
            campaign_goal_status = False
            campaign_recommend_status = False
            campaign_posts_status = False
            posts_history_status = False
            campaign_dashboard_status = False
            quick_posts_status = False
            cammi_assistant_status = False
            image_generation_status = False

        dashboard_url = "https://dev.d58o9xmomxg8r.amplifyapp.com/callback"
        #dashboard_url = "http://localhost:3000/callback"

        redirect_query_params = {
            "token": access_token,
            "name": final_name,
            "email": normalized_email,
            "picture": id_info.get("picture"),
            "sub": id_info.get("sub"),
            "session_id": session_id,
            "onboarding_status": frontend_onboarding_status,
            "locale": id_info.get("locale"),
            "access_token": access_token,
            "expiry": expiry_value,
            "id": id,
            "dashboard_status": dashboard_status,
            "chat_status": chat_status,
            "chatfact_status": chatfact_status,
            "breakdown_status": breakdown_status,
            "generation_status": generation_status,
            "generation_completed_status": generation_completed_status,
            "submit_for_review_status": submit_for_review_status,
            "connector_status": connector_status,
            "add_profile_status": add_profile_status,
            "campaign_start_status": campaign_start_status,
            "campaign_goal_status": campaign_goal_status,
            "campaign_recommend_status": campaign_recommend_status,
            "campaign_posts_status": campaign_posts_status,
            "posts_history_status": posts_history_status,
            "campaign_dashboard_status": campaign_dashboard_status,
            "quick_posts_status": quick_posts_status,
            "cammi_assistant_status": cammi_assistant_status,
            "image_generation_status": image_generation_status
        }

        redirect_url = dashboard_url + "?" + urlencode(redirect_query_params)

        return {
            "statusCode": 302,
            "headers": {
                "Location": redirect_url,
                **CORS_HEADERS
            },
            "body": ""
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e), "event": event})
        }

# ------------------------
# Main Lambda handler
# ------------------------
def lambda_handler(event, context):
    path = event.get("requestContext", {}).get("http", {}).get("path", "") \
        or event.get("path", "")

    if path.endswith("/auth/google-login"):
        return login_lambda(event, context)
    elif path.endswith("/auth/google-callback"):
        return callback_lambda(event, context)
    else:
        return {
            "statusCode": 404,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "Not found"})
        }
