import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key

# ---------- Config ----------
SUPPORT_TABLE_NAME = "email-support-table"
USERS_TABLE_NAME = "users-table"
COUNTER_TABLE_NAME = "ticket-counter-table"
SESSION_GSI_NAME = "session_id-index"

SES_SENDER = "info@cammi.ai"
SUPPORT_EMAIL = "info@cammi.ai"

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
support_table = dynamodb.Table(SUPPORT_TABLE_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)
counter_table = dynamodb.Table(COUNTER_TABLE_NAME)
ses = boto3.client("ses")


# ---------- Atomic Ticket ID Generator ----------
def generate_ticket_id():
    response = counter_table.update_item(
        Key={"counter_name": "support_ticket"},
        UpdateExpression="ADD current_value :inc",
        ExpressionAttributeValues={":inc": 1},
        ReturnValues="UPDATED_NEW"
    )

    ticket_number = int(response["Attributes"]["current_value"])
    return f"T-{ticket_number:03d}" if ticket_number < 1000 else f"T-{ticket_number}"


# ---------- Get User by Session ID ----------
def get_user_by_session_id(session_id):
    response = users_table.query(
        IndexName=SESSION_GSI_NAME,
        KeyConditionExpression=Key("session_id").eq(session_id),
        Limit=1
    )

    items = response.get("Items", [])
    if not items:
        raise Exception("Invalid or expired session_id")

    return items[0]


# ---------- SES Email Helper ----------
def send_support_emails(ticket_id, user_name, user_email, message):
    # Email to Company
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SUPPORT_EMAIL]},
        Message={
            "Subject": {"Data": f"[Support Ticket {ticket_id}] New Ticket"},
            "Body": {
                "Text": {
                    "Data": (
                        f"A new support ticket has been created.\n\n"
                        f"Ticket Number: {ticket_id}\n"
                        f"Name: {user_name}\n"
                        f"Email: {user_email}\n\n"
                        f"Message:\n{message}"
                    )
                }
            }
        },
        ReplyToAddresses=[user_email]
    )

    # Email to User
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [user_email]},
        Message={
            "Subject": {"Data": f"[Support Ticket {ticket_id}] Confirmation"},
            "Body": {
                "Text": {
                    "Data": (
                        f"Hello {user_name},\n\n"
                        f"Your support ticket {ticket_id} has been received. "
                        f"Our team will contact you soon.\n\n"
                        f"Thank you!"
                    )
                }
            }
        },
        ReplyToAddresses=[SUPPORT_EMAIL]
    )


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        # Parse input
        body = json.loads(event["body"]) if "body" in event else event

        session_id = body.get("session_id")
        name = body.get("name")
        phone = body.get("phone")
        email = body.get("email")
        message = body.get("message")

        if not all([session_id, name, phone, email, message]):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required fields"})
            }

        # Fetch user using session_id
        user = get_user_by_session_id(session_id)
        user_id = user.get("id")

        if not user_id:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "User not found for session"})
            }

        # Generate ticket ID (SAFE)
        ticket_id = generate_ticket_id()
        created_at = datetime.utcnow().isoformat()

        # Save ticket
        support_table.put_item(
            Item={
                "user_id": user_id,
                "ticket_id": ticket_id,
                "name": name,
                "phone": phone,
                "email": email,
                "message": message,
                "status": "OPEN",
                "created_at": created_at
            }
        )

        # Send emails
        send_support_emails(ticket_id, name, email, message)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Support ticket created successfully",
                "ticket_id": ticket_id
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
