import json
import boto3
import os
import re
from datetime import datetime
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

BUCKET_NAME = "cammi-devprod"
DEFAULT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"

users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")


def llm_calling(prompt: str, model_id: str) -> str:
    """Call Bedrock LLM and return the response text"""
    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=[
            {
                "role": "user",
                "content": [{"text": prompt}]
            }
        ],
        inferenceConfig={
            "maxTokens": 4000,
            "temperature": 0.6,
            "topP": 0.9
        }
    )
    return response["output"]["message"]["content"][0]["text"]


def extract_json_from_response(response_text):
    """Extract JSON from text that might contain explanatory content"""
    if not response_text or not response_text.strip():
        print("Empty response from LLM")
        return None
    
    # Clean the response text
    response_text = response_text.strip()
    
    # Method 1: Try to find JSON between triple backticks with json marker
    json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
    matches = re.findall(json_pattern, response_text, re.IGNORECASE)
    
    for match in matches:
        try:
            cleaned = match.strip()
            return json.loads(cleaned)
        except json.JSONDecodeError:
            continue
    
    # Method 2: Try to find content between { and } that spans multiple lines
    # This finds the outermost JSON object
    brace_pattern = r'(\{(?:[^{}]|(?:\{[^{}]*\}))*\})'
    matches = re.findall(brace_pattern, response_text, re.DOTALL)
    
    for match in matches:
        try:
            cleaned = match.strip()
            return json.loads(cleaned)
        except json.JSONDecodeError:
            continue
    
    # Method 3: Try to parse the entire response
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        # Method 4: Try to find anything that looks like JSON by finding first { and last }
        first_brace = response_text.find('{')
        last_brace = response_text.rfind('}')
        
        if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
            json_str = response_text[first_brace:last_brace + 1]
            try:
                return json.loads(json_str)
            except json.JSONDecodeError:
                pass
    
    # If all methods fail, log and return None
    print(f"Failed to extract JSON from response. First 500 chars: {response_text[:500]}")
    return None


def lambda_handler(event, context):
    """Main Lambda handler"""
    try:
        # Parse the incoming request
        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_id = body.get("project_id")
        campaign_id = body.get("campaign_id")
        campaign_goal_type = body.get("campaign_goal_type")
        platform_name = body.get("platform_name")
        brand_tone_input = body.get("brand_tone")

        # Validate required fields
        if not all([session_id, project_id, campaign_id, campaign_goal_type]):
            return build_response(400, {"error": "Missing required fields"})

        # Get user from session
        user_resp = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id),
            Limit=1
        )

        if not user_resp.get("Items"):
            return build_response(404, {"error": "User not found"})

        user = user_resp["Items"][0]
        user_id = user["id"]

        # Update user with campaign
        users_table.update_item(
            Key={"email": user["email"]},
            UpdateExpression="ADD campaigns :c",
            ExpressionAttributeValues={
                ":c": {campaign_id}
            }
        )

        # Fetch campaign details
        campaign_resp = campaigns_table.get_item(
            Key={
                "campaign_id": campaign_id,
                "project_id": project_id
            }
        )

        campaign_item = campaign_resp.get("Item", {})
        campaign_name = campaign_item.get("campaign_name", "")

        # Get campaign context from S3
        s3_key = f"knowledgebase/{user_id}/{user_id}_campaign_data.txt"
        try:
            s3_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            campaign_context = s3_obj["Body"].read().decode("utf-8")
        except Exception as e:
            print(f"Error reading from S3: {str(e)}")
            campaign_context = "No context provided."

        # Enhanced prompt with stronger JSON instructions
        prompt = f"""
You are a senior execution-ready social media strategist with deep expertise in LinkedIn campaigns.

Campaign Goal Type:
{campaign_goal_type}

Primary Platform:
LinkedIn

Campaign Context:
You will be provided with a file or block of text as campaign context.
You MUST consider ONLY the latest or final paragraph in the provided content as the main and useful source of information.
Ignore all earlier paragraphs completely.
Use ONLY the last paragraph as the authoritative input and parse it carefully.

CONTEXT:
{campaign_context}

TASK:
Generate a complete campaign execution plan for a LinkedIn social media campaign focused on promoting the given product or company.
Ensure the plan aligns strictly with the campaign goal type and the provided context.

CRITICAL OUTPUT RULES:
- Return ONLY valid JSON with no additional text, explanations, or markdown formatting
- Do NOT wrap the JSON in code blocks or backticks
- Do NOT include any text before or after the JSON
- The response must start with {{ and end with }}
- Do NOT add, remove, or rename any keys
- Do NOT include null values; use realistic, execution-ready values
- Your response must be parseable by json.loads() - no comments, no extra text

Required JSON format (copy this exact structure, only replacing the values):
{{
  "campaign_duration_days": 30,
  "best_suited_platform": "LinkedIn",
  "campaign_type": {{
    "brand_tone": "Professional",
    "brand_voice": "Authoritative",
    "key_message": "Innovative solutions for modern businesses"
  }},
  "post_volume": {{
    "total_posts": 15,
    "posts_per_week": 3
  }},
  "best_posting_time": "09:00 AM EST",
  "creative_brief": "A series of posts highlighting product benefits and industry expertise"
}}

Remember: Your entire response must be a single JSON object that can be parsed with json.loads().
"""

        # Get LLM response with retry logic
        max_retries = 3
        generated_campaign = None
        
        for attempt in range(max_retries):
            print(f"Attempt {attempt + 1} to get LLM response")
            
            try:
                llm_response = llm_calling(prompt, DEFAULT_MODEL_ID)
                print(f"LLM Response (first 200 chars): {llm_response[:200]}")
                
                # Try to extract JSON
                generated_campaign = extract_json_from_response(llm_response)
                
                if generated_campaign and isinstance(generated_campaign, dict):
                    # Validate required fields
                    required_keys = ["campaign_duration_days", "best_suited_platform", "campaign_type", "post_volume", "best_posting_time", "creative_brief"]
                    if all(key in generated_campaign for key in required_keys):
                        print("Successfully parsed valid JSON")
                        break
                    else:
                        print(f"Missing required keys. Found: {list(generated_campaign.keys())}")
                        generated_campaign = None
                else:
                    print(f"Failed to extract JSON on attempt {attempt + 1}")
                    
            except Exception as e:
                print(f"Error on attempt {attempt + 1}: {str(e)}")
            
            if attempt == max_retries - 1:
                # Use default values if all retries fail
                print("All retries failed, using default values")
                generated_campaign = {
                    "campaign_duration_days": 30,
                    "best_suited_platform": platform_name or "LinkedIn",
                    "campaign_type": {
                        "brand_tone": brand_tone_input or "Professional",
                        "brand_voice": "Professional and authoritative",
                        "key_message": f"Campaign message for {campaign_name or 'your brand'}"
                    },
                    "post_volume": {
                        "total_posts": 15,
                        "posts_per_week": 3
                    },
                    "best_posting_time": "09:00 AM EST",
                    "creative_brief": f"Creative brief for {campaign_goal_type} campaign on {platform_name or 'LinkedIn'}"
                }

        # Extract values from generated campaign
        campaign_type_obj = generated_campaign.get("campaign_type", {})
        campaign_duration_days = generated_campaign.get("campaign_duration_days")
        best_suited_platform = generated_campaign.get("best_suited_platform")

        brand_tone = campaign_type_obj.get("brand_tone")
        brand_voice = campaign_type_obj.get("brand_voice")
        key_message = campaign_type_obj.get("key_message")

        post_volume_obj = generated_campaign.get("post_volume", {})
        total_posts = post_volume_obj.get("total_posts")
        posts_per_week = post_volume_obj.get("posts_per_week")

        best_posting_time = generated_campaign.get("best_posting_time")
        creative_brief = generated_campaign.get("creative_brief")

        # Update campaign in DynamoDB
        update_time = datetime.utcnow().isoformat()
        
        campaigns_table.update_item(
            Key={
                "campaign_id": campaign_id,
                "project_id": project_id
            },
            UpdateExpression="""
                SET
                    user_id = :user_id,
                    campaign_goal_type = :campaign_goal_type,
                    platform_name = :platform_name,
                    campaign_duration_days = :campaign_duration_days,
                    best_suited_platform = :best_suited_platform,
                    brand_tone = :brand_tone,
                    brand_voice = :brand_voice,
                    key_message = :key_message,
                    total_posts = :total_posts,
                    posts_per_week = :posts_per_week,
                    best_posting_time = :best_posting_time,
                    creative_brief = :creative_brief,
                    updated_at = :updated_at
            """,
            ExpressionAttributeValues={
                ":user_id": user_id,
                ":campaign_goal_type": campaign_goal_type,
                ":platform_name": best_suited_platform,
                ":campaign_duration_days": campaign_duration_days,
                ":best_suited_platform": best_suited_platform,
                ":brand_tone": brand_tone,
                ":brand_voice": brand_voice,
                ":key_message": key_message,
                ":total_posts": total_posts,
                ":posts_per_week": posts_per_week,
                ":best_posting_time": best_posting_time,
                ":creative_brief": creative_brief,
                ":updated_at": update_time
            }
        )

        # Return success response
        return build_response(
            200,
            {
                "message": "Cammi is analyzing your input",
                "campaign_name": campaign_name,
                "campaign_goal_type": campaign_goal_type,
                "data": generated_campaign
            }
        )
        
    except Exception as e:
        # Log the full error for debugging
        print(f"Unhandled error in lambda_handler: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Return error response
        return build_response(
            500,
            {
                "error": "Internal server error",
                "error_message": str(e)
            }
        )


def build_response(status: int, body: dict):
    """Build HTTP response with CORS headers"""
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }