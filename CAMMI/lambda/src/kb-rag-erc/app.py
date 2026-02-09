import json
import os
import boto3
import logging
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key

# -------------------------------------------------
# Logging
# -------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------------------------------
# Environment Variables
# -------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
KNOWLEDGE_BASE_ID = os.environ.get("KNOWLEDGE_BASE_ID", "EPWIATHAOK")
CAMPAIGN_TABLE = os.environ.get("CAMPAIGN_TABLE", "user-campaigns")

LLM_MODEL_ID = os.environ.get(
    "LLM_MODEL_ID",
    "anthropic.claude-3-sonnet-20240229-v1:0"
)

# -------------------------------------------------
# AWS Clients
# -------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name=REGION)
bedrock_runtime = boto3.client("bedrock-runtime", region_name=REGION)
bedrock_agent_runtime = boto3.client(
    "bedrock-agent-runtime",
    region_name=REGION
)

campaign_table = dynamodb.Table(CAMPAIGN_TABLE)

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def get_campaign_and_user(campaign_id: str) -> dict:
    response = campaign_table.query(
        KeyConditionExpression=Key("campaign_id").eq(campaign_id),
        Limit=1
    )

    items = response.get("Items", [])
    if not items:
        raise ValueError("Campaign not found")

    return items[0]


def retrieve_with_filter(user_id: str, query: str, max_results: int = 5):
    """Primary retrieval attempt with metadata filter"""
    return bedrock_agent_runtime.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={
            "vectorSearchConfiguration": {
                "numberOfResults": max_results,
                "filter": {
                    "equals": {
                        "key": "user_id",
                        "value": user_id
                    }
                }
            }
        }
    )


def retrieve_without_filter(query: str, max_results: int = 5):
    """Fallback retrieval (no filter)"""
    return bedrock_agent_runtime.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={
            "vectorSearchConfiguration": {
                "numberOfResults": max_results
            }
        }
    )


def extract_chunks_and_log_metadata(response: dict):
    results = response.get("retrievalResults", [])

    for r in results:
        logger.info("KB METADATA FOUND: %s", json.dumps(r.get("metadata", {})))

    return [
        r["content"]["text"]
        for r in results
        if r.get("content", {}).get("text")
    ]


def call_llm(system_prompt: str, context: str) -> str:
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 900,
        "temperature": 0.4,
        "system": system_prompt,
        "messages": [
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": context}
                ]
            }
        ]
    }

    response = bedrock_runtime.invoke_model(
        modelId=LLM_MODEL_ID,
        body=json.dumps(payload)
    )

    result = json.loads(response["body"].read())
    return result["content"][0]["text"]

# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        # -----------------------------
        # Parse Input
        # -----------------------------
        if "body" in event:
            body = json.loads(event["body"])
        else:
            body = event

        campaign_id = body.get("campaign_id")
        if not campaign_id:
            return {"statusCode": 400, "message": "campaign_id is required"}

        # -----------------------------
        # 1️⃣ Fetch Campaign
        # -----------------------------
        campaign = get_campaign_and_user(campaign_id)
        user_id = campaign["user_id"]

        logger.info("Campaign resolved | user_id=%s", user_id)

        # -----------------------------
        # 2️⃣ Retrieve Knowledge (FILTERED)
        # -----------------------------
        retrieval_prompt = (
            "Generate execution-ready social media campaign strategy including "
            "content ideas, messaging, platform plan, CTA, and posting schedule."
        )

        logger.info("Attempting KB retrieval WITH metadata filter")

        filtered_response = retrieve_with_filter(user_id, retrieval_prompt)
        chunks = extract_chunks_and_log_metadata(filtered_response)

        # -----------------------------
        # 3️⃣ Fallback if filter fails
        # -----------------------------
        if not chunks:
            logger.warning(
                "No chunks found with metadata filter. Falling back to unfiltered retrieval."
            )

            unfiltered_response = retrieve_without_filter(retrieval_prompt)
            chunks = extract_chunks_and_log_metadata(unfiltered_response)

        if not chunks:
            return {
                "statusCode": 404,
                "message": "Knowledge Base contains no retrievable content"
            }

        context_text = "\n\n".join(chunks)

        # -----------------------------
        # 4️⃣ System Prompt
        # -----------------------------
        system_prompt = f"""
You are a senior digital marketing strategist.

Campaign Name: {campaign.get("campaign_name")}
Campaign Goal: {campaign.get("campaign_goal_type")}
Primary Platform: {campaign.get("platform_name")}

Generate an execution-ready campaign plan using ONLY the provided knowledge.

Output:
1. Objective
2. Target Audience
3. Content Pillars
4. Platform Execution
5. CTA
6. Posting Schedule
"""

        # -----------------------------
        # 5️⃣ Call LLM
        # -----------------------------
        recommendations = call_llm(system_prompt, context_text)

        # -----------------------------
        # 6️⃣ Persist Result
        # -----------------------------
        campaign_table.update_item(
            Key={
                "campaign_id": campaign["campaign_id"],
                "project_id": campaign["project_id"]
            },
            UpdateExpression="SET execution_ready_recommendations = :val",
            ExpressionAttributeValues={":val": recommendations}
        )

        return {
            "statusCode": 200,
            "campaign_id": campaign_id,
            "project_id": campaign["project_id"],
            "user_id": user_id,
            "recommendations": recommendations
        }

    except ClientError as e:
        logger.error("AWS error", exc_info=True)
        return {"statusCode": 500, "message": str(e)}

    except Exception as e:
        logger.error("Unhandled error", exc_info=True)
        return {"statusCode": 500, "message": str(e)}
