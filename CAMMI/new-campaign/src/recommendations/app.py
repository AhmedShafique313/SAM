import json, boto3, os
from boto3.dynamodb.conditions import Attr

s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
lambda_client = boto3.client("lambda")
BUCKET_NAME = "cammi-devprod" 
 
def llm_calling(prompt, model_id):
    conversation = [
        {
            "role": "user",
            "content": [{"text": str(prompt)}]
        }
    ]
    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=conversation,
        inferenceConfig={
            "maxTokens": 60000,
            "temperature": 0.7,
            "topP": 0.9
        }
    )
    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text.strip()
 
def get_http_method(event):
    if "httpMethod" in event:
        return event["httpMethod"]
    return event.get("requestContext", {}).get("http", {}).get("method", "")
 
 
def lambda_handler(event, context):
    method = get_http_method(event)

    if method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    if event.get("action") == "process":
        user_input = event["user_input"]
        campaign_name = event["campaign_name"]
        model_id = event.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")
 
        prompt_relevancy = f"""
You are a senior business and marketing analyst with deep experience in execution-ready B2B and B2C social media campaigns.

You are given raw, unstructured user input that may include ideas, descriptions, opinions, or partial information:
{str(user_input)}

Your task is to carefully READ and EXTRACT only the information that is EXPLICITLY stated by the user.
Do NOT infer, assume, guess, or generate missing details.

Extract the following information, only if it is clearly present in the input:

1. The specific product or service the user wants to promote.
2. The ideal customer described or implied by the user.
3. The main problem or pain point this product or service solves.
4. The key reason the user believes customers should choose them over competitors.
5. The social media platform(s) where the user indicates their audience spends time.
6. The action the user wants people to take after seeing the ad.
7. Any existing creatives or brand assets mentioned (e.g., logos, videos, testimonials).
8. How the user defines success for this campaign (business outcomes, not vanity metrics).

Additional Instructions:
- Preserve the user's original wording as much as possible.
- Maintain the implied brand tone and brand voice.
- If a specific item is NOT mentioned, clearly write: "Not provided by the user."
- Do NOT summarize, rewrite, or improve the content.
- Do NOT add examples or suggestions.
- Use complete sentences.
- Avoid bullet points unless the user explicitly lists items in their input.

Output the extracted information clearly labeled from 1 to 8.
"""

        refined_info = llm_calling(prompt_relevancy, model_id)
 
        s3_key = f"execution - ready campaigns/{campaign_name}/data.txt"

        try:
            existing_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            existing_content = existing_obj["Body"].read().decode("utf-8")
        except s3.exceptions.NoSuchKey:
            existing_content = ""
 
        combined_content = existing_content + "\n\n" + refined_info

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=combined_content.encode("utf-8"),
            ContentType="text/plain"
        )
        return {"ok": True}

    if method == "POST":
        body = json.loads(event.get("body", "{}"))

        user_input = body.get("user_input")
        campaign_name = body.get("campaign_name")
        model_id = body.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")
 
        if not campaign_name or not user_input:
            return build_response(400, {"error": "Missing required fields: campaign_name, user_input"})
 
        payload = {
            "action": "process",               
            "campaign_name": campaign_name,
            "user_input": user_input,
            "model_id": model_id
        }
        lambda_client.invoke(
            FunctionName=context.invoked_function_arn,
            InvocationType="Event",   
            Payload=json.dumps(payload).encode("utf-8")
        )

        return build_response(202, {  
            "message": "Information processing started — Cammi has got your data."
        })
 
    return build_response(405, {"error": "Method not allowed"})
 
 
def build_response(status, body):
    """Helper to build API Gateway compatible response."""
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",  
            "Access-Control-Allow-Headers": "Content-Type",
        },
        "body": json.dumps(body)
    }