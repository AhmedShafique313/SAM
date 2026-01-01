import json
import os
import boto3
from datetime import datetime

# ---------- Config / Environment Variables ----------
TABLE_NAME =  "email-support-table"
SES_SENDER = "info@cammi.ai"  # Verified SES sender
SUPPORT_EMAIL = "info@cammi.ai"

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)
ses = boto3.client("ses")


# ---------- Ticket ID Helper ----------
def get_next_available_ticket_id():
    """
    Scans the table, finds used ticket numbers,
    and returns the smallest unused ticket ID.
    """
    used_numbers = set()
    scan_kwargs = {}
    while True:
        response = table.scan(**scan_kwargs)
        for item in response.get("Items", []):
            ticket_id = item.get("ticket_id")
            if ticket_id and ticket_id.startswith("T-"):
                try:
                    number = int(ticket_id.split("-")[1])
                    used_numbers.add(number)
                except ValueError:
                    pass
        if "LastEvaluatedKey" not in response:
            break
        scan_kwargs["ExclusiveStartKey"] = response["LastEvaluatedKey"]

    # Find smallest unused number
    ticket_number = 1
    while ticket_number in used_numbers:
        ticket_number += 1

    # Format ticket ID
    if ticket_number < 1000:
        return f"T-{ticket_number:03d}"
    else:
        return f"T-{ticket_number}"


# ---------- SES Email Helper ----------
def send_support_emails(ticket_id, user_name, user_email, message):
    """
    Sends two emails:
    1. Company notification (Reply-To = user)
    2. User confirmation (Reply-To = company)
    """
    # ----- Email to Company -----
    company_subject = f"[Support Ticket {ticket_id}] New Ticket"
    company_body = (
        f"A new support ticket has been created.\n\n"
        f"Ticket Number: {ticket_id}\n"
        f"Name: {user_name}\n"
        f"Email: {user_email}\n\n"
        f"Message:\n{message}"
    )
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SUPPORT_EMAIL]},
        Message={"Subject": {"Data": company_subject}, "Body": {"Text": {"Data": company_body}}},
        ReplyToAddresses=[user_email]
    )

    # ----- Email to User -----
    user_subject = f"[Support Ticket {ticket_id}] Confirmation"
    user_body = (
        f"Hello {user_name},\n\n"
        f"Your support ticket {ticket_id} has been received. Our team will contact you soon.\n\n"
        f"Thank you!"
    )
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [user_email]},
        Message={"Subject": {"Data": user_subject}, "Body": {"Text": {"Data": user_body}}},
        ReplyToAddresses=[SUPPORT_EMAIL]
    )


# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        # Handle API Gateway or direct invoke
        if "body" in event:
            body = json.loads(event["body"])
        else:
            body = event

        # Extract user input
        user_id = body.get("user_id")
        name = body.get("name")
        phone = body.get("phone")
        email = body.get("email")
        message = body.get("message")

        if not all([user_id, name, phone, email, message]):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing required fields"})
            }

        # Generate sequential ticket ID
        ticket_id = get_next_available_ticket_id()
        created_at = datetime.utcnow().isoformat()

        # Save ticket to DynamoDB
        item = {
            "user_id": user_id,
            "ticket_id": ticket_id,
            "name": name,
            "phone": phone,
            "email": email,
            "message": message,
            "status": "OPEN",
            "created_at": created_at
        }
        table.put_item(Item=item)

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
