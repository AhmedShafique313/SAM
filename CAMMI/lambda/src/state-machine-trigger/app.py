import json
import boto3
from boto3.dynamodb.conditions import Key

s3 = boto3.client("s3")
BUCKET_NAME = "cammi-devprod"

# -------------------------------
# DynamoDB Resources
# -------------------------------
dynamodb = boto3.resource("dynamodb")
users_table = dynamodb.Table("users-table")
project_state_table = dynamodb.Table("project-state-table")
facts_table = dynamodb.Table("facts-table")

# -------------------------------
# Document Requirements Dictionary
# -------------------------------
DOCUMENT_REQUIREMENTS = {
    "ICP": {
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
    "ICP2": {
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
    "MESSAGING": {
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
    "BRAND": {
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
    "MR": {
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
    "KMF": {
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
    "SR": {
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
    "SMP": {
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
    "GTM": {
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
            "revenue.pricing_position"
        ]
    },
    "BS": {
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
    }
}

# -------------------------------
# Lambda Handler
# -------------------------------
def lambda_handler(event, context):

    # ✅ CORS Headers
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "OPTIONS,POST"
    }

    # ✅ Handle Preflight OPTIONS Request
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps({"message": "CORS preflight successful"})
        }

    try:
        # 1️⃣ Get input
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)

        session_id = body.get("session_id")
        project_id = body.get("project_id")

        if not session_id or not project_id:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Missing session_id or project_id"})
            }

        # 2️⃣ Get user_id from users-table using session_id
        response = users_table.query(
            IndexName="session_id-index",
            KeyConditionExpression=Key("session_id").eq(session_id)
        )

        if not response.get("Items"):
            return {
                "statusCode": 404,
                "headers": headers,
                "body": json.dumps({"error": "User not found for session_id"})
            }

        user = response["Items"][0]
        user_id = user.get("email")

        # 3️⃣ Get active_document for project_id
        project_state = project_state_table.get_item(
            Key={"project_id": project_id}
        )

        if "Item" not in project_state:
            return {
                "statusCode": 404,
                "headers": headers,
                "body": json.dumps({"error": "Project not found"})
            }

        active_document_type = project_state["Item"].get("active_document")
        if not active_document_type or active_document_type not in DOCUMENT_REQUIREMENTS:
            return {
                "statusCode": 400,
                "headers": headers,
                "body": json.dumps({"error": "Invalid or missing active_document"})
            }

        # 4️⃣ Get required and supporting facts
        facts_config = DOCUMENT_REQUIREMENTS[active_document_type]
        all_facts = facts_config["required_facts"] + facts_config["supporting_facts"]

        # 5️⃣ Fetch facts from facts-table
        facts_data = {}
        for fact_id in all_facts:
            fact_item = facts_table.get_item(
                Key={
                    "project_id": project_id,
                    "fact_id": fact_id
                }
            )
            if "Item" in fact_item:
                facts_data[fact_id] = fact_item["Item"]["value"]
            else:
                facts_data[fact_id] = None

        # -----------------------------------
        # Format non-null facts
        # -----------------------------------
        formatted_lines = []
        document_type = active_document_type.lower()

        for fact_id, value in facts_data.items():
            if value:
                parts = fact_id.split(".")
                if len(parts) > 1:
                    fact_name = parts[1]
                else:
                    fact_name = fact_id

                clean_value = str(value).replace(",", "")
                formatted_lines.append(f"{fact_name}: {clean_value}")

        formatted_text = "\n".join(formatted_lines)

        # -----------------------------------
        # Upload file to S3
        # -----------------------------------
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key="latestbusinessidea.txt",
            Body=formatted_text.encode("utf-8"),
            ContentType="text/plain",
            Metadata={
                "token": session_id,
                "project_id": project_id,
                "user_id": user_id,
                "document_type": active_document_type
            }
        )

        # 6️⃣ Return response
        result = {
            "message": "File uploaded successfully to S3",
            "user_id": user_id,
            "project_id": project_id,
            "document_type": active_document_type,
            "document_name": facts_config["name"],
            "facts": facts_data
        }

        return {
            "statusCode": 200,
            "headers": headers,
            "body": json.dumps(result)
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "headers": headers,
            "body": json.dumps({"error": str(e)})
        }
