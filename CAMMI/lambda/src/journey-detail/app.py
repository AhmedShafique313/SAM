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
FACTS_TABLE = "facts-table"

# -------------------------------------------------
# AWS Clients
# -------------------------------------------------
dynamodb = boto3.resource("dynamodb", region_name=REGION)
documents_history_table = dynamodb.Table(DOCUMENTS_HISTORY_TABLE)
facts_table = dynamodb.Table(FACTS_TABLE)

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

# -------------------------------------------------
# Document Requirements (Facts)
# -------------------------------------------------
DOCUMENT_REQUIREMENTS = {
    "icp": {
        "name": "Ideal Customer Profile",
        "required_facts": [
            "customer.primary_customer", "customer.buyer_roles",
            "customer.industries", "customer.company_size",
            "customer.geography", "customer.buyer_goals",
            "customer.buyer_pressures", "customer.problems"
        ],
        "supporting_facts": [
            "customer.information_sources", "customer.current_solutions",
            "market.alternatives"
        ]
    },
    "icp2": {
        "name": "Persona Deep Dive",
        "required_facts": [
            "customer.decision_maker", "customer.buyer_roles",
            "customer.buyer_goals", "customer.buyer_pressures",
            "customer.industries", "customer.company_size",
            "customer.geography"
        ],
        "supporting_facts": [
            "customer.information_sources", "customer.current_solutions"
        ]
    },
    "messaging": {
        "name": "Messaging Document",
        "required_facts": [
            "product.value_proposition_long", "product.unique_differentiation",
            "customer.primary_customer", "customer.buyer_roles",
            "customer.problems", "brand.tone_personality"
        ],
        "supporting_facts": [
            "brand.values_themes", "brand.key_messages", "market.alternatives"
        ]
    },
    "brand": {
        "name": "Brand",
        "required_facts": [
            "business.description_long", "brand.mission",
            "brand.vision", "brand.tone_personality",
            "brand.values_themes", "product.unique_differentiation"
        ],
        "supporting_facts": [
            "brand.vibes_to_avoid", "brand.key_messages"
        ]
    },
    "mr": {
        "name": "Market Research",
        "required_facts": [
            "customer.problems", "customer.current_solutions",
            "market.alternatives", "market.why_alternatives_fail",
            "market.competitors"
        ],
        "supporting_facts": [
            "market.trends_or_shifts", "market.market_size_estimate",
            "market.opportunities", "market.threats"
        ]
    },
    "kmf": {
        "name": "Key Messaging Framework",
        "required_facts": [
            "business.description_long", "product.value_proposition_short",
            "product.unique_differentiation", "customer.primary_customer",
            "customer.problems", "brand.tone_personality"
        ],
        "supporting_facts": [
            "brand.values_themes", "brand.key_messages"
        ]
    },
    "sr": {
        "name": "Strategy Roadmap",
        "required_facts": [
            "strategy.short_term_goals", "strategy.long_term_vision",
            "strategy.priorities", "business.stage"
        ],
        "supporting_facts": [
            "strategy.marketing_objectives", "strategy.user_growth_priorities",
            "business.start_date", "business.end_date_or_milestone"
        ]
    },
    "smp": {
        "name": "Strategic Marketing Plan",
        "required_facts": [
            "business.description_short", "product.value_proposition_short",
            "customer.primary_customer", "customer.problems",
            "strategy.long_term_vision"
        ],
        "supporting_facts": [
            "strategy.success_definition", "strategy.marketing_objectives"
        ]
    },
    "gtm": {
        "name": "Go-to-Market Plan",
        "required_facts": [
            "business.description_long", "product.core_offering",
            "product.unique_differentiation", "customer.primary_customer",
            "customer.industries", "customer.geography",
            "strategy.short_term_goals", "strategy.gtm_focus",
            "market.competitors"
        ],
        "supporting_facts": [
            "strategy.marketing_objectives", "strategy.marketing_tools",
            "market.opportunities", "market.threats",
            "market.market_size_estimate",
            "revenue.pricing_position"
        ]
    },
    "bs": {
        "name": "Brand Strategy",
        "required_facts": [
            "business.name", "business.description_long",
            "product.core_offering", "customer.primary_customer",
            "market.competitors"
        ],
        "supporting_facts": [
            "assets.approved_customers", "assets.case_studies",
            "assets.quotes", "assets.brag_points",
            "assets.spokesperson_name", "assets.spokesperson_role",
            "assets.visual_assets"
        ]
    },
    "cc": {
        "name": "Content Calendar",
        "required_facts": [
            "content.calendar_timeframe", "customer.primary_customer",
            "content.channels", "content.funnel_stages_priority",
            "strategy.marketing_objectives"
        ],
        "supporting_facts": [
            "content.special_activities", "content.pillars",
            "content.formats", "strategy.marketing_tools",
            "brand.tone_personality"
        ]
    },
    "qmp": {
        "name": "Quarterly Marketing Plan",
        "required_facts": [
            "strategy.quarter_timeframe", "strategy.quarterly_goals",
            "strategy.kpi_targets", "strategy.marketing_team_structure",
            "business.name"
        ],
        "supporting_facts": [
            "strategy.quarterly_special_activities", "strategy.short_term_goals",
            "strategy.priorities", "strategy.marketing_objectives",
            "strategy.marketing_budget"
        ]
    },
    "cb": {
        "name": "Campaign Brief",
        "required_facts": []
    },
    "seo": {
        "name": "SEO Strategy",
        "required_facts": []
    },
    "website-landing-page": {
        "name": "Website Landing Page",
        "required_facts": []
    },
    "blog": {
        "name": "Blog Post",
        "required_facts": []
    },
    "social-media-post": {
        "name": "Social Media Post",
        "required_facts": []
    },
    "email-templates": {
        "name": "Email Templates",
        "required_facts": []
    },
    "case-studies": {
        "name": "Case Studies",
        "required_facts": []
    },
    "sales-deck": {
        "name": "Sales Deck",
        "required_facts": []
    },
    "one-pager": {
        "name": "One Pager",
        "required_facts": []
    },
    "dashboard": {
        "name": "Dashboard",
        "required_facts": []
    },
    "recommendation": {
        "name": "Recommendation",
        "required_facts": []
    },
    "updated-assets": {
        "name": "Updated Assets",
        "required_facts": []
    },
    "advice": {
        "name": "Advice",
        "required_facts": []
    }
}

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


def get_facts_by_project(project_id: str) -> list:
    """
    Query facts-table using project_id as partition key
    Returns list of fact items for the given project
    """
    try:
        response = facts_table.query(
            KeyConditionExpression=Key("project_id").eq(project_id)
        )
        return response.get("Items", [])
    except ClientError as e:
        logger.error(f"Error querying facts: {e}")
        raise


def extract_document_types(documents: list) -> set:
    """
    Extract unique document types from the documents list
    Returns set of document type strings
    """
    document_types = set()
    for doc in documents:
        doc_type_uuid = doc.get("document_type_uuid", "")
        if doc_type_uuid:
            document_types.add(doc_type_uuid)
    return document_types


def extract_fact_ids(facts: list) -> set:
    """
    Extract unique fact IDs from the facts list
    Returns set of fact_id strings (e.g., "customer.primary_customer")
    """
    fact_ids = set()
    for fact in facts:
        fact_id = fact.get("fact_id", "")
        if fact_id:
            fact_ids.add(fact_id)
    return fact_ids


def calculate_document_completion(document_type: str, available_facts: set) -> dict:
    """
    Calculate completion percentage for a specific document
    Returns dict with completion details
    """
    doc_requirements = DOCUMENT_REQUIREMENTS.get(document_type.lower(), {})
    required_facts = doc_requirements.get("required_facts", [])

    if not required_facts:
        # If no required facts defined, consider it 100% (or 0% if you prefer)
        return {
            "required_facts_total": 0,
            "required_facts_present": 0,
            "completion_percentage": 0.0
        }

    # Count how many required facts are present
    present_count = sum(1 for fact in required_facts if fact in available_facts)
    total_count = len(required_facts)

    percentage = round((present_count / total_count) * 100, 2) if total_count > 0 else 0.0

    return {
        "required_facts_total": total_count,
        "required_facts_present": present_count,
        "completion_percentage": percentage
    }


def build_document_details(category_id: str, document_types: set, available_facts: set) -> list:
    """
    Build detailed information for each document in the category
    Returns list of document detail dictionaries
    """
    category_docs = CATEGORIES.get(category_id, [])
    document_details = []

    for doc_type in category_docs:
        # Check if document is created
        is_created = any(doc_type in dt or dt.startswith(doc_type) for dt in document_types)

        # Get document name
        doc_info = DOCUMENT_REQUIREMENTS.get(doc_type.lower(), {})
        doc_name = doc_info.get("name", doc_type.upper())

        # Calculate completion
        completion_info = calculate_document_completion(doc_type, available_facts)

        document_details.append({
            "document_type": doc_type,
            "document_name": doc_name,
            "is_created": is_created,
            "completion_percentage": completion_info["completion_percentage"],
            "required_facts_total": completion_info["required_facts_total"],
            "required_facts_present": completion_info["required_facts_present"]
        })

    return document_details


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
        category_id = body.get("category_id")

        # Validate required parameters
        if not project_id:
            return api_response(400, {"message": "project_id is required"})

        if not session_id:
            return api_response(400, {"message": "session_id is required"})

        if not category_id:
            return api_response(400, {"message": "category_id is required"})

        if category_id not in CATEGORIES:
            return api_response(400, {"message": f"Invalid category_id. Must be one of: {list(CATEGORIES.keys())}"})

        logger.info(f"Processing request | session_id={session_id}, project_id={project_id}, category_id={category_id}")

        # Get all documents for the project
        documents = get_documents_by_project(project_id)
        logger.info(f"Found {len(documents)} documents for project_id={project_id}")

        # Get all facts for the project
        facts = get_facts_by_project(project_id)
        logger.info(f"Found {len(facts)} facts for project_id={project_id}")

        # Extract document types and fact IDs
        document_types = extract_document_types(documents)
        available_facts = extract_fact_ids(facts)

        logger.info(f"Unique document types: {document_types}")
        logger.info(f"Available fact IDs: {available_facts}")

        # Get category documents
        category_docs = CATEGORIES[category_id]
        total_documents = len(category_docs)

        # Count completed documents (created in documents-history-table)
        completed_documents = sum(
            1 for doc_type in category_docs
            if any(doc_type in dt or dt.startswith(doc_type) for dt in document_types)
        )

        # Calculate category percentage
        category_percentage = round((completed_documents / total_documents) * 100, 2) if total_documents > 0 else 0.0

        # Build document details
        document_details = build_document_details(category_id, document_types, available_facts)

        # Prepare response
        response_data = {
            "session_id": session_id,
            "project_id": project_id,
            "category": category_id,
            "total_documents": total_documents,
            "completed_documents": completed_documents,
            "percentage": category_percentage,
            "documents": document_details
        }

        logger.info("Successfully calculated journey details")
        return api_response(200, response_data)

    except ClientError as e:
        logger.error("AWS error", exc_info=True)
        return api_response(500, {"message": f"Database error: {str(e)}"})

    except Exception as e:
        logger.error("Unhandled error", exc_info=True)
        return api_response(500, {"message": f"Internal server error: {str(e)}"})

