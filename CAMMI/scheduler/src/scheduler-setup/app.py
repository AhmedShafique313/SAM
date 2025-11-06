import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('organizations-table')

# ✅ Define your campaign questions
CAMPAIGN_QUESTIONS = [
    "What is the main goal of this campaign?",
    "What is the primary role or title of your target audience?",
    "What is the company size of your target audience?",
    "What industry or sector are they in?",
    "What is the campaign theme you want to focus on?",
    "What is your hero message or core positioning statement?",
    "What is your landing page URL (UTM-ready)?",
    "If you don’t have one, what base URL should I use to build UTM-tagged links for each platform?",
    "What proof points or key statistics do you want highlighted?"
]

def lambda_handler(event, context):
    try:
        # ✅ Handle CORS preflight (OPTIONS)
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                    "Access-Control-Allow-Headers": "Content-Type,Authorization"
                },
                "body": json.dumps({"message": "CORS preflight success"})
            }

        # ✅ Validate HTTP method
        if event.get("httpMethod") != "POST":
            return {
                "statusCode": 405,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Method not allowed"})
            }

        # ✅ Parse body
        body = json.loads(event.get("body", "{}"))
        organization_id = body.get("organization_id")
        session_id = body.get("session_id")

        if not organization_id:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"error": "Missing organization_id"})
            }

        # ✅ Get organization record
        response = table.get_item(Key={"id": organization_id})
        item = response.get("Item")

        if not item:
            print(f"No organization found for id: {organization_id}")
            return {
                "statusCode": 404,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"message": "Not found"})
            }

        # ✅ Check post_question_flag
        if not item.get("post_question_flag", False):
            print(f"post_question_flag is False for org: {organization_id}")
            return {
                "statusCode": 404,
                "headers": {
                    "Access-Control-Allow-Origin": "*"
                },
                "body": json.dumps({"message": "Not found"})
            }

        # ✅ Return questions with CORS headers
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization"
            },
            "body": json.dumps({
                "session_id": session_id,
                "organization_id": organization_id,
                "questions": CAMPAIGN_QUESTIONS
            })
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
                "Access-Control-Allow-Headers": "Content-Type,Authorization"
            },
            "body": json.dumps({"error": str(e)})
        }
