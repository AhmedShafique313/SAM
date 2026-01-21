import json
import boto3
import os
from datetime import datetime
from boto3.dynamodb.conditions import Key
from hyperbrowser import Hyperbrowser
from hyperbrowser.models import StartScrapeJobParams, ScrapeOptions

s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb")

users_table = dynamodb.Table("users-table")
campaigns_table = dynamodb.Table("user-campaigns")

BUCKET_NAME = "cammi-devprod"
client_scraper = Hyperbrowser(api_key=os.environ["HYPERBROWSER_API_KEY"])


def build_response(status, body):
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


def get_http_method(event):
    if "httpMethod" in event:
        return event["httpMethod"]
    return event.get("requestContext", {}).get("http", {}).get("method", "")


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

    item = response.get("Item")
    if not item or "campaign_name" not in item:
        raise Exception("campaign_name not found for campaign_id")

    return item["campaign_name"]


def scrape_links(url):
    result = client_scraper.scrape.start_and_wait(
        StartScrapeJobParams(
            url=url,
            scrape_options=ScrapeOptions(formats=["links"], only_main_content=True)
        )
    )
    return result.data.links


def scrape_page_content(url):
    result = client_scraper.scrape.start_and_wait(
        StartScrapeJobParams(
            url=url,
            scrape_options=ScrapeOptions(formats=["markdown"], only_main_content=True)
        )
    )
    return result.data.markdown or ""


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

    # =======================
    # API GATEWAY POST (Synchronous scraping)
    # =======================
    if method == "POST":
        body = json.loads(event.get("body", "{}"))

        session_id = body.get("session_id")
        project_id = body.get("project_id")
        campaign_id = body.get("campaign_id")
        website = body.get("website")
        model_id = body.get(
            "model_id",
            "us.anthropic.claude-sonnet-4-20250514-v1:0"
        )

        if not all([session_id, project_id, campaign_id, website]):
            return build_response(400, {
                "error": "session_id, project_id, campaign_id, and website are required"
            })

        # Get user
        user_resp = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id),
            Limit=1
        )

        if not user_resp.get("Items"):
            return build_response(404, {"error": "User not found"})

        user_id = user_resp["Items"][0]["id"]

        # Mark initial status
        update_campaign_status(
            campaign_id,
            project_id,
            user_id,
            status="Web Scrapped",
            website=website
        )

        # Get campaign_name from DynamoDB
        campaign_name = get_campaign_name(campaign_id, project_id)

        # ----------------------------
        # START SYNCHRONOUS SCRAPING
        # ----------------------------
        links = scrape_links(website)
        links = [l for l in links if l.startswith(website)]

        all_content = ""
        for link in links:
            page_content = scrape_page_content(link)
            all_content += f"\n\n--- Page: {link} ---\n{page_content}"

        structured_info = llm_calling(
            f"Convert this into structured information:\n{all_content}",
            model_id
        )

        final_output = llm_calling(
            f"Convert this into execution-ready campaign data:\n{structured_info}",
            model_id
        )

        # Save to S3
        s3_key = f"knowledgebase/{user_id}/{user_id}_campaign_data.txt"

        try:
            existing_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            existing_content = existing_obj["Body"].read().decode("utf-8")
        except s3.exceptions.NoSuchKey:
            existing_content = ""

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=(existing_content + "\n\n" + final_output).encode("utf-8"),
            ContentType="text/plain"
        )

        # Update status as completed
        update_campaign_status(
            campaign_id,
            project_id,
            user_id,
            status="Web Scrapped Completed"
        )

        # Return synchronous response to frontend
        return build_response(200, {
            "message": "Web scraping completed",
            "s3_path": f"s3://{BUCKET_NAME}/{s3_key}"
        })

    return build_response(405, {"error": "Method not allowed"})
