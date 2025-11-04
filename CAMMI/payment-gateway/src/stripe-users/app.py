import boto3
from datetime import datetime, timezone, timedelta

# DynamoDB setup
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")

def lambda_handler(event, context):
    updated_users = []

    for record in event.get("Records", []):
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue  # only process new items

        new_item = record["dynamodb"].get("NewImage", {})
        if not new_item:
            continue

        # Extract values
        email = new_item.get("email", {}).get("S")
        amount_total = new_item.get("amount_total", {}).get("N")
        plan_name = new_item.get("plan_name", {}).get("S")
        payment_status = new_item.get("payment_status", {}).get("S")
        credits = new_item.get("credits", {}).get("N")
        country = new_item.get("country", {}).get("S")
        currency = new_item.get("currency", {}).get("S")
        payment_at = new_item.get("payment_at", {}).get("N")
        lookup_key = new_item.get("lookup_key", {}).get("S")

        if not email:
            continue

        # ✅ Convert Unix timestamp to PKT formatted date-time (stored in payment_at)
        if payment_at:
            pkt = timezone(timedelta(hours=5))
            dt_pkt = datetime.fromtimestamp(int(payment_at), pkt)
            payment_at = dt_pkt.strftime("%Y-%m-%d %I:%M:%S %p PKT")

        # Build update expression
        update_expr = []
        expr_values = {}

        if amount_total is not None:
            update_expr.append("amount_total = :amount_total")
            expr_values[":amount_total"] = int(amount_total)

        if plan_name is not None:
            update_expr.append("plan_name = :plan_name")
            expr_values[":plan_name"] = plan_name

        if payment_status is not None:
            update_expr.append("payment_status = :payment_status")
            expr_values[":payment_status"] = payment_status

        if credits is not None:
            update_expr.append("credits = :credits")
            expr_values[":credits"] = int(credits)

            update_expr.append("total_credits = if_not_exists(total_credits, :zero) + :credits")
            expr_values[":zero"] = 0
        
        if country is not None:
            update_expr.append("country = :country")
            expr_values[":country"] = country
        
        if currency is not None:
            update_expr.append("currency = :currency")
            expr_values[":currency"] = currency

        if payment_at is not None:
            update_expr.append("payment_at = :payment_at")
            expr_values[":payment_at"] = payment_at

        if lookup_key is not None:
            update_expr.append("lookup_key = :lookup_key")
            expr_values[":lookup_key"] = lookup_key

        if update_expr:  # only run if something to update
            users_table.update_item(
                Key={"email": email},
                UpdateExpression="SET " + ", ".join(update_expr),
                ExpressionAttributeValues=expr_values
            )

            updated_users.append({
                "email": email,
                "amount_total": amount_total,
                "plan_name": plan_name,
                "payment_status": payment_status,
                "credits": credits,
                "country": country,
                "currency": currency,
                "payment_at": payment_at,
                "lookup_key": lookup_key
            })

    return {
        "statusCode": 200,
        "body": f"Updated {len(updated_users)} user(s)",
        "updated_users": updated_users
    }
