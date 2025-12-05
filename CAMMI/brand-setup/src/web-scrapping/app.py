import json, boto3, os
from boto3.dynamodb.conditions import Attr
from hyperbrowser import Hyperbrowser
from hyperbrowser.models import StartScrapeJobParams, ScrapeOptions

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name="us-east-1")

# --- Configuration ---
BUCKET_NAME = "cammi-devprod"
users_table = dynamodb.Table("users-table")
client_scraper = os.environ["HYPERBROWSER_API_KEY"]


def scrape_links(url):
    """Scrape all links from the website."""
    result = client_scraper.scrape.start_and_wait(
        StartScrapeJobParams(
            url=url,
            scrape_options=ScrapeOptions(formats=["links"], only_main_content=True)
        )
    )
    return result.data.links


def scrape_page_content(url):
    """Scrape markdown content from a given URL."""
    result = client_scraper.scrape.start_and_wait(
        StartScrapeJobParams(
            url=url,
            scrape_options=ScrapeOptions(formats=["markdown"], only_main_content=True)
        )
    )
    return result.data.markdown or ""


def llm_calling(prompt, model_id, session_id="default-session"):
    """Call AWS Bedrock LLM. (No try/except — errors will propagate for visibility.)"""
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
        },
        requestMetadata={"sessionId": session_id}
    )

    response_text = response["output"]["message"]["content"][0]["text"]
    return response_text.strip()


def get_http_method(event):
    if "httpMethod" in event:
        return event["httpMethod"]
    return event.get("requestContext", {}).get("http", {}).get("method", "")


def lambda_handler(event, context):
    method = get_http_method(event)

    # CORS preflight
    if method == "OPTIONS":
        return build_response(200, {"message": "CORS preflight OK"})

    # Background worker
    if event.get("action") == "process":
        session_id = event["session_id"]
        website = event["website"]
        project_id = event["project_id"]
        model_id = event.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")

        user_resp = users_table.scan(
            ProjectionExpression="id, email",
            FilterExpression=Attr("session_id").eq(session_id)
        )
        user_items = user_resp.get("Items", [])
        if not user_items:
            return {"ok": False, "reason": "User not found"}

        user = user_items[0]
        user_id = user.get("id")
        email = user.get("email")

        links = scrape_links(website)
        links = [link for link in links if link.startswith(website)]

        all_content = ""
        for idx, link in enumerate(links, start=1):
            page_content = scrape_page_content(link)
            all_content += f"\n\n--- Page: {link} ---\n{page_content}"

        prompt_structuring = f"""
You are an expert information architect.
Convert the unstructured data below into structured information.
Do not remove any information — just present it in a structured format.

Data:
{str(all_content)}
"""

        prompt_relevancy = f"""
You are an expert business and marketing analyst specializing in B2B brand strategy.

You are given structured company information (scraped and pre-organized in JSON or markdown):
{str(all_content)}

Your task:
1. Extract all key information relevant to building a detailed and personalized business profile.
2. Use only factual data found in the input. Do not infer or invent data.
3. Return the response in the exact format below using the same headings and order.
4. If any field cannot be determined confidently, leave it blank.

Return your answer in this format exactly:

Business Name:
Industry / Sector:
Mission:
Vision:
Objective / Purpose Statement:
Business Concept:
Products or Services Offered:
Target Market:
Who They Currently Sell To:
Value Proposition:
Top Business Goals:
Challenges:
Company Overview / About Summary:
Core Values / Brand Personality:
Unique Selling Points (USPs):
Competitive Advantage / What Sets Them Apart:
Market Positioning Statement:
Customer Segments:
Proof Points / Case Studies / Testimonials Summary:
Key Differentiators:
Tone of Voice / Brand Personality Keywords:
Core Features / Capabilities:
Business Model:
Technology Stack / Tools / Platform:
Geographic Presence:
Leadership / Founder Info:
Company Values / Culture:
Strategic Initiatives / Future Plans:
Awards / Recognition / Partnerships:
Press Mentions or Achievements:
Client or Industry Verticals Served:
"""

        structured_info = llm_calling(prompt_structuring, model_id, session_id)
        relevant_info = llm_calling(prompt_relevancy, model_id, session_id)

        prompt_finalize = f"""
You are an expert business analyst.
Using the structured information below, create a professional company profile.

{str(relevant_info)}

Return the output using these exact headings:
Business Name:
Industry / Sector:
Mission:
Vision:
Products or Services Offered:
Target Market:
Value Proposition:
Company Overview:

Please return the response in plain text format. Do not use markdown.
"""

        finalize_info = llm_calling(prompt_finalize, model_id, session_id)

        s3_key = f"url_parsing/{project_id}/{user_id}/web_scraping.txt"

        try:
            existing_obj = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)
            existing_content = existing_obj["Body"].read().decode("utf-8")
        except s3.exceptions.NoSuchKey:
            existing_content = ""

        combined_content = existing_content + "\n\n" + finalize_info

        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=combined_content.encode("utf-8"),
            ContentType="text/plain"
        )

        return {"ok": True}

    if method == "POST":
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        website = body.get("website")
        project_id = body.get("project_id")
        model_id = event.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")

        if not session_id or not website or not project_id:
            return build_response(400, {"error": "Missing required fields: session_id, project_id, or website"})

        payload = {
            "action": "process",
            "session_id": session_id,
            "website": website,
            "project_id": project_id,
            "model_id": model_id
        }

        lambda_client.invoke(
            FunctionName=context.invoked_function_arn,
            InvocationType="Event",
            Payload=json.dumps(payload).encode("utf-8")
        )

        return build_response(202, {
            "message": "Scraping started — Cammi has got your data."
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
