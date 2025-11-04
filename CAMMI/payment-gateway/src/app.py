import json
import stripe
import boto3
import os

secrets_client = boto3.client("secretsmanager")
 
def get_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"]) if "SecretString" in response else None

# -------------------------
# Environment Configuration
# -------------------------
stripe.api_key = get_secret(os.environ["STRIPE_API_KEY"])
FRONTEND_DOMAIN = "https://nonoppressive-undyingly-thatcher.ngrok-free.dev"

# DynamoDB setup
dynamodb = boto3.resource("dynamodb")
stripe_table = dynamodb.Table("stripe_table")

# -------------------------
# Plan Mapping (lookup_key → plan_name, credits)
# -------------------------
PLAN_CREDITS = {
    # Monthly subscription plans
    "explorer_monthly": {"plan_name": "Explorer", "credits": 500},
    "starter_monthly": {"plan_name": "Starter", "credits": 5000},
    "growth_monthly": {"plan_name": "Growth", "credits": 20000},
    "pro_monthly": {"plan_name": "Pro", "credits": 50000},
    "scale_enterprise_monthly": {"plan_name": "Scale/Enterprise", "credits": 150000},
    # Annually subscription plans
    "explorer_annually": {"plan_name": "Explorer", "credits": 6000},
    "starter_annually": {"plan_name": "Starter", "credits": 60000},
    "growth_annually": {"plan_name": "Growth", "credits": 240000},
    "pro_annually": {"plan_name": "Pro", "credits": 600000},
    "scale_enterprise_annually": {"plan_name": "Scale/Enterprise", "credits": 1800000},

    # Custom / one-time plan
    "agency_custom": {"plan_name": "Agency/Custom", "credits": 1000},
}



# -------------------------
# Lambda Handler
# -------------------------
def lambda_handler(event, context):
    path = event.get("path", "")
    method = event.get("httpMethod", "GET")

    # ------------------------
    # 1️⃣ Create Checkout Session
    # ------------------------
    if path.endswith("/checkout-plans") and method == "POST":
        body = parse_body(event)
        lookup_key = body.get("lookup_key")

        if not lookup_key:
            return response_json({"error": "lookup_key required"}, 400)

        prices = stripe.Price.list(lookup_keys=[lookup_key], expand=["data.product"])
        if not prices.data:
            return response_json({"error": "Invalid lookup_key"}, 400)

        checkout_session = stripe.checkout.Session.create(
            line_items=[{"price": prices.data[0].id, "quantity": 1}],
            mode="subscription",
            # discounts=[{"promotion_code": "promo_1SFwsN1LHsiGbvuai4RHkxRb"}],
            success_url=f"{FRONTEND_DOMAIN}/success?session_id={{CHECKOUT_SESSION_ID}}",
            cancel_url=f"{FRONTEND_DOMAIN}/cancel",
            metadata={"lookup_key": lookup_key},  # store lookup_key for webhook
        )

        return response_json({"checkout_url": checkout_session.url})

    # ------------------------
    # 2️⃣ Create Customer Portal
    # ------------------------
    elif path.endswith("/create-portal-session") and method == "POST":
        body = parse_body(event)
        session_id = body.get("session_id")

        if not session_id:
            return response_json({"error": "session_id required"}, 400)

        checkout_session = stripe.checkout.Session.retrieve(session_id)
        if not checkout_session.customer:
            return response_json({"error": "No customer found for this session"}, 400)

        portal_session = stripe.billing_portal.Session.create(
            customer=checkout_session.customer,
            return_url=FRONTEND_DOMAIN,
        )

        return response_json({"portal_url": portal_session.url})

    # ------------------------
    # 3️⃣ Handle Webhook (Stripe → Lambda)
    # ------------------------
    elif path.endswith("/payments") and method == "POST":
        webhook_secret = "whsec_lUfZEYFvE2yNQUTjaRPoETYRFytxXK45"
        payload = event.get("body", "")
        sig_header = event["headers"].get("Stripe-Signature")

        # ⚠️ No try/except — directly parse webhook
        stripe_event = stripe.Webhook.construct_event(
            payload=payload,
            sig_header=sig_header,
            secret=webhook_secret
        )

        event_type = stripe_event["type"]
        data = stripe_event["data"]["object"]

        # ✅ Process only successful events
        if event_type in ("checkout.session.completed", "payment_intent.succeeded"):
            customer_email = (
                data.get("customer_details", {}).get("email")
                or data.get("receipt_email")
                or data.get("metadata", {}).get("email")
            )

            if not customer_email:
                print("⚠️ No email found in webhook payload")
                return response_json({"status": "ignored", "reason": "no email"}, 200)

            # Detect lookup_key
            lookup_key = None
            if data.get("metadata") and "lookup_key" in data["metadata"]:
                lookup_key = data["metadata"]["lookup_key"]
            elif data.get("subscription"):
                subscription = stripe.Subscription.retrieve(data["subscription"])
                if subscription["items"]["data"]:
                    lookup_key = subscription["items"]["data"][0]["price"].get("lookup_key")

            # Fallback if lookup_key not found
            plan_info = PLAN_CREDITS.get(lookup_key, {"plan_name": "Agency/Custom", "credits": 1000})

            # Build DynamoDB record
            db_item = {
                "email": customer_email,
                "delivery_status": "success",
                "stripe_event_type": event_type,
                "payment_id": stripe_event.get("id"),
                "payment_at": data.get("created"),
                "amount_subtotal": data.get("amount_subtotal"),
                "amount_total": data.get("amount_total"),
                "currency": data.get("currency"),
                "customer_id": data.get("customer"),
                "country": data.get("customer_details", {}).get("address", {}).get("country") if data.get("customer_details") else None,
                "business_name": data.get("customer_details", {}).get("business_name") if data.get("customer_details") else None,
                "name": data.get("customer_details", {}).get("name") if data.get("customer_details") else None,
                "phone": data.get("customer_details", {}).get("phone") if data.get("customer_details") else None,
                "invoice_id": data.get("invoice"),
                "package_mode": data.get("mode"),
                "payment_status": data.get("payment_status", "succeeded"),
                "subscription_id": data.get("subscription"),
                "lookup_key": lookup_key,
                "success_url": f"{FRONTEND_DOMAIN}/success?session_id={data.get('id')}",
                "plan_name": plan_info["plan_name"],
                "credits": plan_info["credits"],
                "body": payload,
            }

            # ✅ Write to DynamoDB
            stripe_table.put_item(Item=db_item)
            print(f"✅ Payment recorded for {customer_email} ({plan_info['plan_name']})")

            return response_json({"status": "success", "event_type": event_type}, 200)

        # 💤 Ignore other events but respond 200
        print(f"ℹ️ Ignored event type: {event_type}")
        return response_json({"status": "ignored", "event_type": event_type}, 200)

    # ------------------------
    # 4️⃣ Fallback for unknown routes
    # ------------------------
    return response_json({"error": f"Route {path} not found"}, 404)


# ------------------------
# Helper Functions
# ------------------------
def parse_body(event):
    """Parse JSON body from API Gateway event"""
    if event.get("body"):
        try:
            return json.loads(event["body"])
        except json.JSONDecodeError:
            return {}
    return {}

def response_json(body, status=200):
    """Return JSON response with CORS headers"""
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": FRONTEND_DOMAIN,
            "Access-Control-Allow-Methods": "POST, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body),
    }
