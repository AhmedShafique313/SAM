import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
BUCKET_NAME = "cammi-devprod"
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

def build_response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }

def get_http_method(event):
    if "httpMethod" in event:
        return event["httpMethod"]
    return event.get("requestContext", {}).get("http", {}).get("method", "")

def update_campaign_status(campaign_id, project_id, user_id, status):
    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        },
        UpdateExpression="""
            SET input_data_status = :status,
                user_id = :uid,
                updated_at = :updated_at
        """,
        ExpressionAttributeValues={
            ":status": status,
            ":uid": user_id,
            ":updated_at": datetime.utcnow().isoformat()
        }
    )

def get_campaign_name(campaign_id, project_id):
    response = campaigns_table.get_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        }
    )
    item = response.get("Item")
    if not item or "campaign_name" not in item:
        raise Exception("campaign_name not found for campaign_id")
    return item["campaign_name"]

def llm_calling(prompt, model_id):
    response = bedrock_runtime.converse(
        modelId=model_id,
        messages=[{
            "role": "user",
            "content": [{"text": str(prompt)}]
        }],
        inferenceConfig={
            "maxTokens": 60000,
            "temperature": 0.7,
            "topP": 0.9
        }
    )
    return response["output"]["message"]["content"][0]["text"].strip()

def lambda_handler(event, context):
    method = get_http_method(event)

    if method == "OPTIONS":
        return build_response(200, {"message": "CORS OK"})

    if method == "POST":
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        project_id = body.get("project_id")
        campaign_id = body.get("campaign_id")
        user_input = body.get("user_input")
        model_id = body.get(
            "model_id",
            "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )

        if not all([session_id, project_id, campaign_id]):
            return build_response(400, {
                "error": "session_id, project_id, campaign_id are required"
            })

        user_resp = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id),
            Limit=1
        )

        if not user_resp.get("Items"):
            return build_response(404, {"error": "User not found"})

        user_id = user_resp["Items"][0]["id"]

        update_campaign_status(
            campaign_id,
            project_id,
            user_id,
            status="User Input",
        )

        campaign_name = get_campaign_name(campaign_id, project_id)
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
 
        s3_key = f"knowledgebase/{user_id}/{user_id}_campaign_data.txt"

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
            ContentType="text/plain",
            Metadata={"user_id": user_id}
        )

        update_campaign_status(
            campaign_id,
            project_id,
            user_id,
            status="Input Refinement Completed"
        )

        return build_response(200, {
            "message": "Input refinement completed for Execution Ready Campaigns",
            "s3_path": f"s3://{BUCKET_NAME}/{s3_key}"
        })
 
    return build_response(405, {"error": "Method not allowed"})