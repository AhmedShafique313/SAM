import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# ---------- DynamoDB ----------
dynamodb = boto3.resource("dynamodb")

CAMPAIGNS_TABLE = "user-campaigns"
POSTS_TABLE = "posts-table"

PROJECT_GSI = "project_id-index"
CAMPAIGN_GSI = "campaign_id-index"

campaigns_table = dynamodb.Table(CAMPAIGNS_TABLE)
posts_table = dynamodb.Table(POSTS_TABLE)


def lambda_handler(event, context):
    """Fetch campaigns for a project, filter by posts, and update status"""

    # ✅ CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return _response(200, {"message": "CORS preflight"})

    body = event.get("body")
    if body:
        try:
            event = json.loads(body)
        except json.JSONDecodeError:
            return _response(400, {"error": "Invalid JSON in request body"})

    session_id = event.get("session_id")
    project_id = event.get("project_id")

    if not project_id:
        return _response(400, {"error": "project_id is required"})

    try:
        # 1️⃣ Fetch campaigns by project_id
        campaigns = _query_by_project_id(project_id)

        # 2️⃣ Filter campaigns that have posts + update status
        filtered_campaigns = []
        for campaign in campaigns:
            campaign_id = str(campaign.get("campaign_id")).strip()

            posts = _get_posts_for_campaign(campaign_id)

            # ❌ Skip campaigns with NO posts
            if not posts:
                continue

            campaign["total_posts"] = len(posts)

            # 🔄 Determine campaign status
            new_status = _derive_campaign_status(posts)

            if new_status and campaign.get("status") != new_status:
                _update_campaign_status(
                    campaign_id,
                    campaign["project_id"],
                    new_status
                )
                campaign["status"] = new_status

            filtered_campaigns.append(campaign)

        # 3️⃣ Format response
        formatted_campaigns = _format_campaigns(filtered_campaigns)

        return _response(
            200,
            {
                "session_id": session_id,
                "project_id": project_id,
                "campaigns": formatted_campaigns
            }
        )

    except ClientError as e:
        return _response(
            500,
            {
                "error": "DynamoDB operation failed",
                "details": e.response["Error"]["Message"]
            }
        )


# ---------- Helpers ----------

def _query_by_project_id(project_id):
    """Query campaigns using project_id GSI (pagination-safe)"""
    items = []
    last_key = None

    while True:
        params = {
            "IndexName": PROJECT_GSI,
            "KeyConditionExpression": Key("project_id").eq(project_id)
        }

        if last_key:
            params["ExclusiveStartKey"] = last_key

        response = campaigns_table.query(**params)
        items.extend(response.get("Items", []))

        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break

    return items


def _get_posts_for_campaign(campaign_id):
    """Fetch posts for a campaign using campaign_id GSI"""
    print("Querying posts for campaign_id:", repr(campaign_id))

    response = posts_table.query(
        IndexName=CAMPAIGN_GSI,
        KeyConditionExpression=Key("campaign_id").eq(campaign_id)
    )

    return response.get("Items", [])


def _derive_campaign_status(posts):
    """Determine campaign status from posts (case-insensitive)"""

    statuses = {
        str(post.get("status", "")).strip().lower()
        for post in posts
    }

    if statuses.issubset({"generated", "draft"}):
        return "in-progress"

    if statuses == {"scheduled"}:
        return "active"

    if statuses == {"published"}:
        return "completed"

    return None


def _update_campaign_status(campaign_id, project_id, status):
    """Update campaign status in user-campaigns table"""
    campaigns_table.update_item(
        Key={
            "campaign_id": campaign_id,
            "project_id": project_id
        },
        UpdateExpression="SET #s = :s",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":s": status}
    )


def _format_campaigns(items):
    """Return campaigns as numbered dictionary"""
    formatted = {}
    for index, item in enumerate(items, start=1):
        formatted[f"campaign-{index}"] = item
    return formatted


def _response(status_code, body):
    """Standard HTTP response with CORS"""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST,PUT",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body, default=str)
    }
