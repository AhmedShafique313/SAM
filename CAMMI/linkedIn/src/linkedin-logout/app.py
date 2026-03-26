import json
import boto3
import urllib3
import os
from urllib.parse import urlencode

http = urllib3.PoolManager()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('linkedin-user-table')

CLIENT_ID = os.environ.get("L_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("L_CLIENT_SECRET", "")
REVOKE_URL = "https://www.linkedin.com/oauth/v2/revoke"
LINKEDIN_LOGOUT_URL = "https://www.linkedin.com/m/logout"

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "OPTIONS,POST"
}

def lambda_handler(event, context):

    # Handle preflight OPTIONS request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({"message": "CORS preflight success"})
        }

    try:
        # Parse body from frontend request
        body = json.loads(event['body'])
        sub = body.get('sub')

        if not sub:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "message": "sub is required"
                })
            }

        # First, retrieve the user's access token from DynamoDB
        response = table.get_item(Key={'sub': sub})
        user_item = response.get('Item')

        if not user_item:
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({
                    "message": "User not found"
                })
            }

        access_token = user_item.get('access_token')

        revocation_success = False

        # Revoke the token on LinkedIn's side if access token exists
        if access_token:
            try:
                revoke_data = {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "token": access_token
                }
                encoded_data = urlencode(revoke_data)

                revoke_response = http.request(
                    "POST",
                    REVOKE_URL,
                    body=encoded_data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"}
                )

                # LinkedIn revocation endpoint returns 200 on success
                revocation_success = (revoke_response.status == 200)
                print(f"Token revocation status: {revoke_response.status}")
                print(f"Revocation response: {revoke_response.data.decode('utf-8') if revoke_response.data else 'No response body'}")

            except Exception as revoke_error:
                # Log the error but continue with logout
                print(f"Token revocation error: {str(revoke_error)}")
                revocation_success = False

        # Delete item from DynamoDB
        table.delete_item(
            Key={
                'sub': sub
            }
        )

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Logout successful - user fully logged out from LinkedIn",
                "token_revoked": revocation_success,
                "db_record_deleted": True,
                "linkedin_logout_url": LINKEDIN_LOGOUT_URL,
                "instructions": "To complete logout and see login screen again, redirect user to linkedin_logout_url or they must manually logout from LinkedIn"
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Error during logout",
                "error": str(e)
            })
        }