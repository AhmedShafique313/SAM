import json
import os
import boto3
from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    RunRealtimeReportRequest,
    DateRange,
    Metric,
)
from google.oauth2 import service_account

secrets_client = boto3.client("secretsmanager")
 
def get_secret(secret_name):
    response = secrets_client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"]) if "SecretString" in response else None
 
GA_CREDS_JSON = get_secret(os.environ["GA_CREDS_JSON"])

# ---------------------------
# Environment variables
# ---------------------------
ANALYTICS_TABLE_NAME = "wordpress-analytics-table"
PROJECTS_TABLE_NAME = "projects-table"

if not GA_CREDS_JSON:
    raise RuntimeError("GA_CREDS_JSON environment variable is missing")

# ---------------------------
# DynamoDB
# ---------------------------
dynamodb = boto3.resource("dynamodb")
analytics_table = dynamodb.Table(ANALYTICS_TABLE_NAME)
projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)

# ---------------------------
# GA4 Client
# ---------------------------
credentials = service_account.Credentials.from_service_account_info(
    json.loads(GA_CREDS_JSON)
)
ga_client = BetaAnalyticsDataClient(credentials=credentials)

# ---------------------------
# Metrics
# ---------------------------
GA_METRICS = [
    "activeUsers",
    "totalUsers",
    "sessions",
    "screenPageViews",
    "engagedSessions",
    "engagementRate",
    "averageSessionDuration",
    "bounceRate",
    "eventCount",
    "userEngagementDuration",
]

# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):
    try:
        # ---------------------------
        # 1️⃣ Parse BODY ONLY
        # ---------------------------
        if not event.get("body"):
            return response(400, {"error": "Request body is required"})

        body = json.loads(event["body"])

        project_id = body.get("project_id")
        start_date = body.get("start_date")
        end_date = body.get("end_date")

        if not project_id:
            return response(400, {"error": "project_id is required"})

        # Default date range → last 7 days
        start_date = start_date or "7daysAgo"
        end_date = end_date or "today"

        # ---------------------------
        # 2️⃣ Get GA property ID
        # ---------------------------
        analytics_response = analytics_table.get_item(
            Key={"project_id": project_id}
        )

        if "Item" in analytics_response:
            ga_property_id = analytics_response["Item"].get("ga_property_id")
        else:
            project_response = projects_table.get_item(
                Key={"project_id": project_id}
            )

            if "Item" not in project_response:
                return response(404, {"error": "Project ID not found"})

            now_iso = datetime.utcnow().isoformat() + "Z"
            new_entry = {
                "project_id": project_id,
                "integration_type": "GA4",
                "ga_property_id": project_response["Item"].get("ga_property_id"),
                "ga_measurement_id": project_response["Item"].get("ga_measurement_id"),
                "site_url": project_response["Item"].get("site_url", ""),
                "status": "active",
                "created_at": now_iso,
                "updated_at": now_iso,
            }

            analytics_table.put_item(Item=new_entry)
            ga_property_id = new_entry["ga_property_id"]

        if not ga_property_id:
            return response(400, {"error": "GA property ID missing for project"})

        property_path = f"properties/{ga_property_id}"

        # ---------------------------
        # 3️⃣ Delayed GA Report
        # ---------------------------
        report_request = RunReportRequest(
            property=property_path,
            date_ranges=[
                DateRange(
                    start_date=start_date,
                    end_date=end_date,
                )
            ],
            metrics=[Metric(name=m) for m in GA_METRICS],
        )

        report_response = ga_client.run_report(report_request)

        stats = {}
        if report_response.rows:
            row = report_response.rows[0]
            for i, metric_name in enumerate(GA_METRICS):
                value = row.metric_values[i].value
                stats[metric_name] = float(value) if "." in value else int(value)
        else:
            stats = {m: 0 for m in GA_METRICS}

        # ---------------------------
        # 4️⃣ Realtime GA Report
        # ---------------------------
        realtime_request = RunRealtimeReportRequest(
            property=property_path,
            metrics=[Metric(name="activeUsers")],
        )

        realtime_response = ga_client.run_realtime_report(realtime_request)

        realtime_active_users = 0
        if realtime_response.rows:
            realtime_active_users = int(
                realtime_response.rows[0].metric_values[0].value
            )

        # ---------------------------
        # 5️⃣ Final Response
        # ---------------------------
        return response(
            200,
            {
                "project_id": project_id,
                "property_id": ga_property_id,
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date,
                },
                "realtime": {
                    "activeUsers": realtime_active_users
                },
                "stats": stats,
            },
        )

    except Exception as e:
        return response(500, {"error": str(e)})

# ---------------------------
# Helper
# ---------------------------
def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body),
    }
