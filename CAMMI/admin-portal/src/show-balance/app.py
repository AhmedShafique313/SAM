import json
import stripe
import os

stripe.api_key = os.environ["STRIPE_API_KEY"]

def lambda_handler(event, context):
    balance = stripe.Balance.retrieve()

    available_amount = balance["available"][0]["amount"] / 100
    pending_amount = balance["pending"][0]["amount"] / 100
    currency = balance["available"][0]["currency"].upper()

    formatted_balance = {
        "available": f"{available_amount:.2f} {currency}",
        "pending": f"{pending_amount:.2f} {currency}",
        "total": f"{available_amount + pending_amount:.2f} {currency}"
    }

    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(formatted_balance)
    }
