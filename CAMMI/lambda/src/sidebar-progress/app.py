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
DOCUMENTS_HISTORY_TABLE = "documents-history-table"

# -------------------------------------------------
# AWS Clients
# -------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name=REGION)
documents_history_table = dynamodb.Table(DOCUMENTS_HISTORY_TABLE)

# -------------------------------------------------
# Categories and Document Types
# -------------------------------------------------
CATEGORIES = {
    "Clarify": ["icp", "icp2", "messaging", "brand", "mr", "kmf", "sr", "smp", "gtm", "bs"],
    "Align": ["cc", "qmp", "cb", "seo"],
    "Mobilize": ["website-landing-page", "blog", "social-media-post", "email-templates", "case-studies", "sales-deck", "one-pager"],
    "Monitor": ["dashboard"],
    "Iterate": ["recommendation", "updated-assets", "advice"]
}

# Category sequence for sequential unlocking
CATEGORY_SEQUENCE = ["Clarify", "Align", "Mobilize", "Monitor", "Iterate"]

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
def get_documents_by_project(project_id: str) -> list:
    """
    Query documents-history-table using project_id GSI
    Returns list of document items for the given project
    """
    try:
        response = documents_history_table.query(
            IndexName="project-id-doc-index",
            KeyConditionExpression=Key("project_id").eq(project_id)
        )
        return response.get("Items", [])
    except ClientError as e:
        logger.error(f"Error querying documents: {e}")
        raise


def extract_document_types(documents: list) -> set:
    """
    Extract unique document types from the documents list
    Returns set of document type strings
    """
    document_types = set()
    for doc in documents:
        # Extract document_type from document_type_uuid (format: "document_type_uuid")
        doc_type_uuid = doc.get("document_type_uuid", "")
        # The document type is the first part before any delimiter like underscore or UUID
        # Based on the table structure, we need to extract the actual document type
        if doc_type_uuid:
            # Assuming document_type_uuid contains the document type
            # If it's stored differently, adjust accordingly
            document_types.add(doc_type_uuid)

    return document_types


def calculate_category_progress(document_types: set) -> dict:
    """
    Calculate progress percentage and lock status for each category
    Sequential unlocking: each category unlocks when previous category has >= 2 documents
    Returns dict with category name as key and progress info as value
    """
    category_progress = {}

    # First pass: calculate completed documents and percentage for all categories
    for category_name, category_docs in CATEGORIES.items():
        total_docs = len(category_docs)
        completed_docs = 0

        # Check which documents from this category are present
        for doc_type in category_docs:
            # Check if document type exists in the retrieved documents
            # Handle different possible formats (with or without UUID suffix)
            if any(doc_type in dt or dt.startswith(doc_type) for dt in document_types):
                completed_docs += 1

        # Calculate percentage
        percentage = round((completed_docs / total_docs) * 100, 2) if total_docs > 0 else 0

        category_progress[category_name] = {
            "total_documents": total_docs,
            "completed_documents": completed_docs,
            "percentage": percentage,
            "status": "locked"  # Will be updated in next pass
        }

    # Second pass: determine lock status based on sequential unlocking
    for i, category_name in enumerate(CATEGORY_SEQUENCE):
        if i == 0:
            # First category (Clarify) is always unlocked
            category_progress[category_name]["status"] = "unlocked"
        else:
            # Unlock if previous category has >= 2 completed documents
            previous_category = CATEGORY_SEQUENCE[i - 1]
            previous_completed = category_progress[previous_category]["completed_documents"]

            if previous_completed >= 2:
                category_progress[category_name]["status"] = "unlocked"
            else:
                category_progress[category_name]["status"] = "locked"

    return category_progress


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

        # Validate required parameters
        if not project_id:
            return api_response(400, {"message": "project_id is required"})

        if not session_id:
            return api_response(400, {"message": "session_id is required"})

        logger.info(f"Processing request | session_id={session_id}, project_id={project_id}")

        # Get all documents for the project
        documents = get_documents_by_project(project_id)
        logger.info(f"Found {len(documents)} documents for project_id={project_id}")

        # Extract document types
        document_types = extract_document_types(documents)
        logger.info(f"Unique document types: {document_types}")

        # Calculate progress for each category
        category_progress = calculate_category_progress(document_types)

        # Calculate overall progress
        total_categories = len(CATEGORIES)
        unlocked_categories = sum(1 for cat in category_progress.values() if cat["status"] == "unlocked")
        overall_percentage = round((unlocked_categories / total_categories) * 100, 2) if total_categories > 0 else 0

        # Prepare response
        response_data = {
            "session_id": session_id,
            "project_id": project_id,
            "overall_progress": {
                "total_categories": total_categories,
                "unlocked_categories": unlocked_categories,
                "percentage": overall_percentage
            },
            "category_progress": category_progress
        }

        logger.info("Successfully calculated progress")
        return api_response(200, response_data)

    except ClientError as e:
        logger.error("AWS error", exc_info=True)
        return api_response(500, {"message": f"Database error: {str(e)}"})

    except Exception as e:
        logger.error("Unhandled error", exc_info=True)
        return api_response(500, {"message": f"Internal server error: {str(e)}"})
