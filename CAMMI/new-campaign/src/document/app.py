import json
import boto3
import pdfplumber
import io
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

BUCKET_NAME = "cammi-devprod"
campaigns_table = dynamodb.Table("user-campaigns")

# -----------------------------------------
# LLM helpers
# -----------------------------------------
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
            "temperature": 0.7,
            "topP": 0.9
        }
    )
    return response["output"]["message"]["content"][0]["text"].strip()


def call_llm_extract_profile(all_content: str) -> str:
    prompt = f"""
You are an expert business and marketing analyst specializing in B2B brand strategy.

Input:
{all_content}

Extract factual company profile only.
Do not infer or invent data.

Return in exact structured format.
""".strip()

    return llm_calling(
        prompt,
        model_id="us.anthropic.claude-sonnet-4-20250514-v1:0"
    )

# -----------------------------------------
# DynamoDB helpers
# -----------------------------------------
def update_campaign_status(campaign_id, project_id, user_id, status, website=None):
    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        },
        UpdateExpression="""
            SET input_data_status = :status,
                user_id = :uid,
                website = if_not_exists(website, :website),
                updated_at = :updated_at
        """,
        ExpressionAttributeValues={
            ":status": status,
            ":uid": user_id,
            ":website": website or "",
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
    return response.get("Item", {}).get("campaign_name")

# -----------------------------------------
# Lambda handler (EventBridge)
# -----------------------------------------
def lambda_handler(event, context):
    print("📥 Received EventBridge event:", json.dumps(event))

    # -------------------------------------
    # Extract EventBridge S3 details
    # -------------------------------------
    detail = event.get("detail", {})
    bucket_name = detail.get("bucket", {}).get("name")
    object_key = detail.get("object", {}).get("key")

    if not bucket_name or not object_key:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid EventBridge S3 event"})
        }

    if bucket_name != BUCKET_NAME:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Unexpected bucket: {bucket_name}"})
        }

    print(f"📄 New object detected: s3://{bucket_name}/{object_key}")

    # -------------------------------------
    # Expected S3 path:
    # pdf_files/{project_id}/{user_id}/{campaign_id}/{file_name}
    # -------------------------------------
    parts = object_key.split("/")

    if len(parts) < 5 or parts[0] != "pdf_files":
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Invalid S3 key structure: {object_key}"})
        }

    project_id = parts[1]
    user_id = parts[2]
    campaign_id = parts[3]
    file_name = parts[4]

    # -------------------------------------
    # Update campaign status
    # -------------------------------------
    update_campaign_status(
        campaign_id=campaign_id,
        project_id=project_id,
        user_id=user_id,
        status="PDF Extracted"
    )

    campaign_name = get_campaign_name(campaign_id, project_id)
    print(f"🎯 Campaign: {campaign_name}, File: {file_name}")

    # -------------------------------------
    # Read PDF
    # -------------------------------------
    pdf_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
    pdf_bytes = pdf_obj["Body"].read()

    all_text = ""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                all_text += text + "\n" + "-" * 80 + "\n"

    # -------------------------------------
    # Call LLM
    # -------------------------------------
    parsed_profile = call_llm_extract_profile(all_text)

    # -------------------------------------
    # Write to knowledgebase (user-scoped)
    # -------------------------------------
    kb_key = f"knowledgebase/{user_id}/{user_id}_campaign_data.txt"

    try:
        existing = s3.get_object(Bucket=BUCKET_NAME, Key=kb_key)
        existing_content = existing["Body"].read().decode("utf-8")
    except s3.exceptions.NoSuchKey:
        existing_content = ""

    final_output = (
        existing_content + "\n\n--- NEW PDF EXTRACT ---\n\n" + parsed_profile
        if existing_content else parsed_profile
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=kb_key,
        Body=final_output.encode("utf-8"),
        ContentType="text/plain",
        Metadata={
            "user_id": user_id,
            "project_id": project_id,
            "campaign_id": campaign_id
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "EventBridge-triggered PDF processed successfully",
            "campaign_name": campaign_name,
            "knowledgebase_path": f"s3://{BUCKET_NAME}/{kb_key}"
        })
    }
