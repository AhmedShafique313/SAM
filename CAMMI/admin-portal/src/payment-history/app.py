import boto3
import uuid
from datetime import datetime, timezone, timedelta

# DynamoDB setup
dynamodb = boto3.resource("dynamodb")
payment_history_table = dynamodb.Table("payment_history-table")

def lambda_handler(event, context):
    inserted_records = []

    for record in event.get("Records", []):
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue  # only process new or modified items

        new_item = record["dynamodb"].get("NewImage", {})
        if not new_item:
            continue

        # Extract values
        email = new_item.get("email", {}).get("S")
        name = new_item.get("name", {}).get("S")
        amount_total = new_item.get("amount_total", {}).get("N")
        plan_name = new_item.get("plan_name", {}).get("S")
        payment_status = new_item.get("payment_status", {}).get("S")
        credits = new_item.get("credits", {}).get("N")
        country = new_item.get("country", {}).get("S")
        currency = new_item.get("currency", {}).get("S")
        payment_at = new_item.get("payment_at", {}).get("N")
        lookup_key = new_item.get("lookup_key", {}).get("S")

        # ✅ Convert Unix timestamp → UTC formatted date-time
        if payment_at:
            dt_utc = datetime.fromtimestamp(int(payment_at), tz=timezone.utc)
            payment_at = dt_utc.strftime("%Y-%m-%d %I:%M:%S %p UTC")

        # Generate unique short ID
        index_id = str(uuid.uuid4())[:8]

        # Insert into payment_history
        payment_history_table.put_item(
            Item={
                "index_id": index_id,
                "email": email,
                "name": name,
                "amount_total": int(amount_total) if amount_total else None,
                "plan_name": plan_name,
                "payment_status": payment_status,
                "credits": int(credits) if credits else None,
                "country": country,
                "currency": currency,
                "payment_at": payment_at,
                "lookup_key": lookup_key
            }
        )

        inserted_records.append(index_id)

    # ✅ Minimal clean return (best practice)
    return {"status": "ok", "processed": len(inserted_records)}
