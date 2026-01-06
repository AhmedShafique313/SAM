import json
import os
import boto3
from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    RunReportRequest,
    DateRange,
    Dimension,
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
ANALYTICS_TABLE_NAME = os.environ.get("ANALYTICS_TABLE", "analytics-table")
PROJECTS_TABLE_NAME = os.environ.get("PROJECTS_TABLE", "projects-table")

if not GA_CREDS_JSON:
    raise RuntimeError("GA_CREDS_JSON environment variable is missing")

# ---------------------------
# DynamoDB
# ---------------------------
dynamodb = boto3.resource("dynamodb")
analytics_table = dynamodb.Table(ANALYTICS_TABLE_NAME)
projects_table = dynamodb.Table(PROJECTS_TABLE_NAME)

# ---------------------------
# GA4 Client (reuse)
# ---------------------------
credentials = service_account.Credentials.from_service_account_info(
    json.loads(GA_CREDS_JSON)
)
ga_client = BetaAnalyticsDataClient(credentials=credentials)

# ---------------------------
# Safety limits
# ---------------------------
MAX_DIMENSIONS = 3
MAX_METRICS = 5

DEFAULT_METRICS = [
    "sessions",
    "activeUsers",
    "screenPageViews",
]

# ---------------------------
# Lambda Handler
# ---------------------------
def lambda_handler(event, context):
    try:
        if not event.get("body"):
            return response(400, {"error": "Request body is required"})

        body = json.loads(event["body"])

        project_id = body.get("project_id")
        dimensions = body.get("dimensions", [])
        metrics = body.get("metrics", DEFAULT_METRICS)

        start_date = body.get("start_date", "7daysAgo")
        end_date = body.get("end_date", "today")

        # ---------------------------
        # Debug: log input
        # ---------------------------
        print("DEBUG: Event body:", body)

        if not project_id:
            return response(400, {"error": "project_id is required"})

        if not dimensions or not isinstance(dimensions, list):
            return response(400, {"error": "dimensions must be a non-empty list"})

        if len(dimensions) > MAX_DIMENSIONS:
            return response(400, {
                "error": f"Maximum {MAX_DIMENSIONS} dimensions allowed"
            })

        if len(metrics) > MAX_METRICS:
            return response(400, {
                "error": f"Maximum {MAX_METRICS} metrics allowed"
            })

        # ---------------------------
        # Get GA property ID
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
                return response(404, {"error": "Project not found"})

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
            return response(400, {"error": "GA property ID missing"})

        property_path = f"properties/{ga_property_id}"

        # ---------------------------
        # Build GA4 breakdown report
        # ---------------------------
        report_request = RunReportRequest(
            property=property_path,
            date_ranges=[
                DateRange(
                    start_date=start_date,
                    end_date=end_date
                )
            ],
            dimensions=[Dimension(name=d) for d in dimensions],
            metrics=[Metric(name=m) for m in metrics],
            limit=1000,
        )

        report_response = ga_client.run_report(report_request)

        # ---------------------------
        # Debug: log GA4 raw response
        # ---------------------------
        print("DEBUG: GA4 report_response:", report_response)

        rows = []

        for row in report_response.rows:
            row_data = {}

            # Dimensions
            for i, dim in enumerate(dimensions):
                row_data[dim] = row.dimension_values[i].value

            # Metrics
            for i, metric in enumerate(metrics):
                value = row.metric_values[i].value
                row_data[metric] = float(value) if "." in value else int(value)

            rows.append(row_data)

        # ---------------------------
        # Debug: log processed rows
        # ---------------------------
        print("DEBUG: Processed rows:", rows)

        # ---------------------------
        # Final response
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
                "dimensions": dimensions,
                "metrics": metrics,
                "row_count": len(rows),
                "rows": rows,
            },
        )

    except Exception as e:
        print("DEBUG: Exception:", str(e))
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
