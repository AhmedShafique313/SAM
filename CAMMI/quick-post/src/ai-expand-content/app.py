import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from decimal import Decimal

# Initialize Bedrock Runtime client
bedrock_runtime = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1"
)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
USERS_TABLE = dynamodb.Table('users-table')  # Updated table name

# GSI name for session_id lookup
USER_GSI_NAME = 'session_id-index'  # Replace with your actual GSI name

# Claude 4 Sonnet model ID
MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

# Credit cost per API call
CREDIT_COST = 2

# Common CORS headers
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",  # replace with your domain in production
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-Amz-Date,X-Api-Key,X-Amz-Security-Token",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
    "Access-Control-Allow-Credentials": "true",
    "Content-Type": "application/json"
}


class DecimalEncoder(json.JSONEncoder):
    """Helper class to convert Decimal objects to JSON-serializable format"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super(DecimalEncoder, self).default(obj)


def get_user_by_session(session_id):
    """Get user by session_id using GSI"""
    try:
        res = USERS_TABLE.query(
            IndexName=USER_GSI_NAME,
            KeyConditionExpression=Key("session_id").eq(session_id),
        )
        return res["Items"][0] if res.get("Items") else None
    except Exception as e:
        print(f"Error querying user by session: {str(e)}")
        return None


def get_user_credits(email):
    """Get user's current credit balance"""
    try:
        res = USERS_TABLE.get_item(Key={"email": email})
        user = res.get("Item", {})
        credits = user.get("total_credits", 0)
        # Convert Decimal to int if necessary
        if isinstance(credits, Decimal):
            return int(credits)
        return int(credits)
    except Exception as e:
        print(f"Error getting user credits: {str(e)}")
        return 0


def deduct_credits_atomic(email, deduction):
    """
    Deduct credits safely using DynamoDB ConditionExpression.
    Prevents race conditions and negative credits.
    """
    try:
        response = USERS_TABLE.update_item(
            Key={"email": email},
            UpdateExpression="SET total_credits = total_credits - :d",
            ConditionExpression="total_credits >= :d",
            ExpressionAttributeValues={
                ":d": deduction,
            },
            ReturnValues="UPDATED_NEW"
        )
        
        # Extract and convert the new credit balance
        attributes = response.get('Attributes', {})
        new_balance = attributes.get('total_credits', 0)
        
        # Convert Decimal to int if necessary
        if isinstance(new_balance, Decimal):
            new_balance = int(new_balance)
            
        return True, new_balance
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False, None
        print(f"Error deducting credits: {str(e)}")
        raise


def build_response(status_code, body_dict):
    """Helper function to build consistent API responses with Decimal handling"""
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(body_dict, cls=DecimalEncoder)
    }


def invoke_claude(prompt: str) -> dict:
    """Invoke Claude model with the given prompt"""
    system_prompt = """
You are an expert social media copywriter and SEO strategist.

Your task:
- Analyze the user's idea or content prompt
- Generate ONE social media caption JSON containing:
  1. title: short, catchy post title
  2. description: engaging post description (1–3 sentences)
  3. hashtags: relevant hashtags, all lowercase, separated by a single space

Strict rules:
- Output ONLY valid JSON
- No markdown
- No explanations
- No additional text
- Hashtags must be lowercase and space-separated (NOT an array)

Required JSON schema:
{
  "title": "post title here",
  "description": "post description here",
  "hashtags": "#hashtag1 #hashtag2 #hashtag3"
}
"""

    messages = [
        {
            "role": "user",
            "content": [
                {
                    "text": f"{system_prompt}\n\nUser Input:\n{prompt}"
                }
            ]
        }
    ]

    try:
        response = bedrock_runtime.converse(
            modelId=MODEL_ID,
            messages=messages,
            inferenceConfig={
                "temperature": 0.6,
                "topP": 0.9
            }
        )

        response_text = response["output"]["message"]["content"][0]["text"]
        return json.loads(response_text)
    except Exception as e:
        print(f"Error invoking Claude: {str(e)}")
        raise


def lambda_handler(event, context):
    """
    Main Lambda handler function
    """
    # Handle preflight OPTIONS request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": ""
        }

    # Log the incoming event for debugging
    print(f"Received event: {json.dumps(event)}")

    # Parse and validate request body
    body = event.get("body")
    if not body:
        return build_response(400, {"error": "Request body is required"})
    
    if not isinstance(body, str):
        return build_response(400, {"error": "Request body must be a JSON string"})

    try:
        body_json = json.loads(body)
    except json.JSONDecodeError as e:
        return build_response(400, {"error": f"Invalid JSON in request body: {str(e)}"})

    # Extract session_id from request body
    session_id = body_json.get("session_id", "").strip()
    
    if not session_id:
        return build_response(400, {"error": "Field 'session_id' is required and cannot be empty"})

    # Get user by session_id
    user = get_user_by_session(session_id)

    if not user:
        return build_response(401, {"error": "Invalid session - no user found"})

    # Extract email from user record
    email = user.get("email")
    
    if not email:
        return build_response(500, {"error": "User record missing email field"})

    # Extract prompt from request body
    prompt = body_json.get("prompt")
    if not prompt:
        return build_response(400, {"error": "Field 'prompt' is required"})
    
    if not isinstance(prompt, str):
        return build_response(400, {"error": "Field 'prompt' must be a string"})

    # Check user's current credits
    try:
        current_credits = get_user_credits(email)
    except Exception as e:
        return build_response(500, {"error": f"Failed to retrieve user credits: {str(e)}"})
    
    # Verify sufficient credits
    if current_credits < CREDIT_COST:
        return build_response(
            402,
            {
                "error": "Insufficient credits",
                "remaining_credits": current_credits,
                "required_credits": CREDIT_COST,
                "session_id": session_id
            }
        )

    # Attempt to deduct credits atomically
    try:
        success, new_credit_balance = deduct_credits_atomic(email, CREDIT_COST)
    except Exception as e:
        return build_response(
            500,
            {
                "error": "Failed to process credit deduction",
                "details": str(e)
            }
        )
    
    if not success:
        # Credit deduction failed (likely due to concurrent request or insufficient credits)
        # Get latest credit balance for accurate error message
        try:
            current_credits = get_user_credits(email)
        except:
            current_credits = 0
            
        return build_response(
            402,
            {
                "error": "Insufficient credits",
                "remaining_credits": current_credits,
                "required_credits": CREDIT_COST,
                "session_id": session_id
            }
        )

    # Credits successfully deducted, proceed with model invocation
    try:
        claude_output = invoke_claude(prompt.strip())
        
        # Get updated credits after deduction (if not already returned from deduct_credits_atomic)
        if new_credit_balance is None:
            new_credits = get_user_credits(email)
        else:
            new_credits = new_credit_balance
        
        # Return successful response with caption and remaining credits
        return build_response(
            200,
            {
                "caption": claude_output,
                "remaining_credits": new_credits,
                "session_id": session_id
            }
        )
        
    except json.JSONDecodeError as e:
        # Model returned invalid JSON - need to refund credits
        print(f"Invalid JSON response from Claude: {str(e)}")
        
        # Refund the credits
        try:
            USERS_TABLE.update_item(
                Key={"email": email},
                UpdateExpression="SET total_credits = total_credits + :refund",
                ExpressionAttributeValues={
                    ":refund": CREDIT_COST,
                },
            )
            refunded_credits = get_user_credits(email)
        except Exception as refund_error:
            print(f"CRITICAL: Failed to refund credits for {email}: {str(refund_error)}")
            refunded_credits = None
        
        return build_response(
            500,
            {
                "error": "Model returned invalid response format",
                "details": "Failed to parse model output as JSON",
                "remaining_credits": refunded_credits,
                "session_id": session_id
            }
        )
        
    except Exception as e:
        # Model invocation failed - need to refund the credits
        print(f"Model invocation failed: {str(e)}")
        
        # Refund the credits since the operation failed
        try:
            USERS_TABLE.update_item(
                Key={"email": email},
                UpdateExpression="SET total_credits = total_credits + :refund",
                ExpressionAttributeValues={
                    ":refund": CREDIT_COST,
                },
            )
            # Get updated credits after refund
            refunded_credits = get_user_credits(email)
        except Exception as refund_error:
            # Log the refund error but don't fail the response
            print(f"CRITICAL: Failed to refund credits for {email}: {str(refund_error)}")
            refunded_credits = None
        
        return build_response(
            500,
            {
                "error": "Model invocation failed",
                "details": str(e),
                "remaining_credits": refunded_credits,
                "session_id": session_id
            }
        )