import json
import os
import boto3
import logging
from botocore.exceptions import ClientError
from datetime import datetime

# -------------------------------------------------
# Logging
# -------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# -------------------------------------------------
# Environment Variables
# -------------------------------------------------
REGION = os.environ.get("AWS_REGION", "us-east-1")
CLARIFY_ALIGN_STATE_TABLE = os.environ.get("CLARIFY_ALIGN_STATE_TABLE", "clarify-align-state-table")

# -------------------------------------------------
# AWS Clients
# -------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name=REGION)
clarify_align_state_table = dynamodb.Table(CLARIFY_ALIGN_STATE_TABLE)

# -------------------------------------------------
# API Gateway Response Helper
# -------------------------------------------------
def api_response(status_code: int, body: dict):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST,OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type"
        },
        "body": json.dumps(body)
    }

# -------------------------------------------------
# Helper Functions
# -------------------------------------------------
def upsert_clarify_align_state(project_id: str, session_id: str, current_tab: str):
    """
    Upsert clarify-align-state-table with project_id as partition key
    Updates or creates the record with current_tab and session_id
    """
    try:
        timestamp = datetime.utcnow().isoformat()

        response = clarify_align_state_table.update_item(
            Key={
                "project_id": project_id
            },
            UpdateExpression="SET current_tab = :tab, session_id = :sid, updated_at = :ts",
            ExpressionAttributeValues={
                ":tab": current_tab,
                ":sid": session_id,
                ":ts": timestamp
            },
            ReturnValues="ALL_NEW"
        )

        return response.get("Attributes", {})

    except ClientError as e:
        logger.error(f"Error upserting clarify-align state: {e}")
        raise

# -------------------------------------------------
# Lambda Handler
# -------------------------------------------------
def lambda_handler(event, context):
    try:
        logger.info("Received event: %s", json.dumps(event))

        # Parse request body
        body = json.loads(event.get("body", "{}"))
        session_id = body.get("session_id")
        project_id = body.get("project_id")
        current_tab = body.get("current_tab")

        # Validate required parameters
        if not project_id:
            return api_response(400, {"message": "project_id is required"})

        if not session_id:
            return api_response(400, {"message": "session_id is required"})

        if not current_tab:
            return api_response(400, {"message": "current_tab is required"})

        logger.info(f"Processing request | session_id={session_id}, project_id={project_id}, current_tab={current_tab}")

        # Upsert the state
        updated_item = upsert_clarify_align_state(project_id, session_id, current_tab)

        # Prepare response
        response_data = {
            "message": "State updated successfully",
            "project_id": project_id,
            "session_id": session_id,
            "current_tab": current_tab,
            "updated_at": updated_item.get("updated_at")
        }

        logger.info("Successfully updated clarify-align state")
        return api_response(200, response_data)

    except ClientError as e:
        logger.error("AWS error", exc_info=True)
        return api_response(500, {"message": f"Database error: {str(e)}"})

    except Exception as e:
        logger.error("Unhandled error", exc_info=True)
        return api_response(500, {"message": f"Internal server error: {str(e)}"})
