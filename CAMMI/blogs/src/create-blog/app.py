import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr, Key
from decimal import Decimal


dynamodb = boto3.resource("dynamodb")

USERS_TABLE_NAME = os.environ.get("USERS_TABLE", "users-table")
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")

users_table = dynamodb.Table(USERS_TABLE_NAME)
facts_table = dynamodb.Table(FACTS_TABLE_NAME)


OUTCOME_OPTIONS = [
    {
        "id": "trust_authority",
        "label": "Build trust and authority",
        "description": "Position the brand as credible and reliable.",
    },
    {
        "id": "qualified_leads",
        "label": "Generate qualified leads",
        "description": "Encourage high-intent readers to take action.",
    },
    {
        "id": "discoverability",
        "label": "Improve discoverability",
        "description": "Make content easier to find through search and AI answers.",
    },
    {
        "id": "educate_market",
        "label": "Educate the market",
        "description": "Teach readers about a problem, trend, or method.",
    },
    {
        "id": "launch_announcement",
        "label": "Product or feature announcement",
        "description": "Share a launch, update, or milestone.",
    },
    {
        "id": "thought_leadership",
        "label": "Thought leadership",
        "description": "Share a point of view and original insight.",
    },
    {
        "id": "conversion_demo",
        "label": "Drive demos or conversions",
        "description": "Move readers toward trial, demo, or purchase.",
    },
    {
        "id": "other",
        "label": "Other",
        "description": "Choose this if your outcome is different.",
    },
]

CTA_OPTIONS = [
    {"id": "book_demo", "label": "Book a demo"},
    {"id": "start_trial", "label": "Start free trial"},
    {"id": "contact_sales", "label": "Contact sales"},
    {"id": "download_resource", "label": "Download a resource"},
    {"id": "subscribe_newsletter", "label": "Subscribe to newsletter"},
    {"id": "read_related", "label": "Read a related article"},
    {"id": "reply_comment", "label": "Reply or engage"},
    {"id": "other", "label": "Other"},
]

TONE_OPTIONS = [
    {"id": "expert_strategic", "label": "Expert and strategic"},
    {"id": "friendly_simple", "label": "Friendly and simple"},
    {"id": "bold_opinionated", "label": "Bold and opinionated"},
    {"id": "formal_corporate", "label": "Formal and corporate"},
    {"id": "practical_no_fluff", "label": "Practical and no-fluff"},
    {"id": "other", "label": "Other"},
]

DEPTH_OPTIONS = [
    {"id": "quick_read", "label": "Quick read (700-900 words)"},
    {"id": "standard", "label": "Standard depth (1000-1400 words)"},
    {"id": "deep_dive", "label": "Deep dive (1600-2200 words)"},
]


def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" in event:
        body = event["body"]
        if isinstance(body, str):
            return json.loads(body)
        if isinstance(body, dict):
            return body
        raise ValueError("Invalid request body format")
    return event


def response(status_code: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "body": json.dumps(payload, default=str),
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
    }


def sanitize_from_dynamodb(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    if isinstance(obj, dict):
        return {k: sanitize_from_dynamodb(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_from_dynamodb(i) for i in obj]
    return obj


def get_user_id_from_session(session_id: str) -> Optional[str]:
    try:
        result = users_table.scan(FilterExpression=Attr("session_id").eq(session_id))
        items = result.get("Items", [])
        if not items:
            return None
        return items[0].get("id")
    except Exception:
        return None


def load_facts(project_id: str) -> Dict[str, Dict[str, Any]]:
    facts: Dict[str, Dict[str, Any]] = {}
    response_obj = facts_table.query(KeyConditionExpression=Key("project_id").eq(project_id))

    while True:
        for item in response_obj.get("Items", []):
            fact_id = item.get("fact_id")
            if not fact_id:
                continue
            facts[fact_id] = {
                "value": sanitize_from_dynamodb(item.get("value")),
                "source": item.get("source", "chat"),
                "updated_at": item.get("updated_at"),
            }

        if "LastEvaluatedKey" not in response_obj:
            break

        response_obj = facts_table.query(
            KeyConditionExpression=Key("project_id").eq(project_id),
            ExclusiveStartKey=response_obj["LastEvaluatedKey"],
        )

    return facts


def get_fact_value(facts: Dict[str, Dict[str, Any]], key: str) -> Optional[str]:
    raw = facts.get(key, {}).get("value")
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


def build_company_context(facts: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    # Blog-relevant fact set only. We avoid noisy/internal facts and focus on
    # identity, offer, audience, pain, and messaging signals.
    key_fields = [
        ("company_name", "business.name"),
        ("company_description", "business.description_short"),
        ("industry", "business.industry"),
        ("core_offering", "product.core_offering"),
        ("value_prop", "product.value_proposition_short"),
        ("target_audience", "customer.primary_customer"),
        ("customer_problem", "customer.problems"),
        ("brand_tone", "brand.tone_personality"),
        ("key_messages", "brand.key_messages"),
        ("proof_points", "assets.brag_points"),
    ]

    mapped: Dict[str, Optional[str]] = {}
    for alias, fact_id in key_fields:
        mapped[alias] = get_fact_value(facts, fact_id)

    summary_points: List[str] = []
    if mapped.get("company_name"):
        summary_points.append(f"Company: {mapped['company_name']}")
    if mapped.get("company_description"):
        summary_points.append(f"What they do: {mapped['company_description']}")
    if mapped.get("industry"):
        summary_points.append(f"Industry: {mapped['industry']}")
    if mapped.get("core_offering"):
        summary_points.append(f"Core offering: {mapped['core_offering']}")
    if mapped.get("target_audience"):
        summary_points.append(f"Target audience: {mapped['target_audience']}")
    if mapped.get("customer_problem"):
        summary_points.append(f"Customer problem: {mapped['customer_problem']}")
    if mapped.get("brand_tone"):
        summary_points.append(f"Brand tone: {mapped['brand_tone']}")

    core_signals = [
        mapped.get("company_name"),
        mapped.get("company_description"),
        mapped.get("core_offering"),
        mapped.get("target_audience"),
    ]
    support_signals = [
        mapped.get("customer_problem"),
        mapped.get("brand_tone"),
        mapped.get("value_prop"),
        mapped.get("proof_points"),
    ]

    core_count = sum(1 for x in core_signals if x)
    support_count = sum(1 for x in support_signals if x)
    score = (core_count * 2) + support_count

    if score >= 6:
        context_level = "strong"
    elif score >= 3:
        context_level = "partial"
    else:
        context_level = "low"

    has_minimum_company_context = context_level in ["strong", "partial"]

    return {
        "available": has_minimum_company_context,
        "context_level": context_level,
        "context_score": score,
        "fields": mapped,
        "summary_points": summary_points,
    }


def normalize_tone_to_option(raw_tone: Optional[str]) -> Optional[str]:
    if not raw_tone:
        return None
    lower = raw_tone.lower()
    if any(word in lower for word in ["friendly", "simple", "approachable"]):
        return "friendly_simple"
    if any(word in lower for word in ["bold", "opinion", "provocative"]):
        return "bold_opinionated"
    if any(word in lower for word in ["formal", "corporate", "professional"]):
        return "formal_corporate"
    if any(word in lower for word in ["expert", "strategic", "authoritative"]):
        return "expert_strategic"
    if any(word in lower for word in ["practical", "no fluff", "direct"]):
        return "practical_no_fluff"
    return None


def build_prefilled_fields(facts: Dict[str, Dict[str, Any]], company_context: Dict[str, Any]) -> Dict[str, Any]:
    tone_raw = company_context["fields"].get("brand_tone")
    prefilled = {
        "audience": company_context["fields"].get("target_audience"),
        "suggested_tone_option": normalize_tone_to_option(tone_raw),
        "suggested_tone_raw": tone_raw,
        "marketing_objective_hint": get_fact_value(facts, "strategy.marketing_objectives"),
        "key_message_hint": company_context["fields"].get("key_messages"),
    }
    return prefilled


def build_questions(
    facts: Dict[str, Dict[str, Any]],
    company_context: Dict[str, Any],
    prefilled: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    questions: List[Dict[str, Any]] = []
    notes: List[str] = []

    # Always ask topic: this is too specific to infer safely.
    questions.append(
        {
            "id": "blog_topic",
            "type": "text_long",
            "required": True,
            "title": "Blog Topic",
            "prompt": "What exact topic do you want this blog to cover?",
            "placeholder": "Example: 7 ways B2B SaaS teams can improve LinkedIn lead quality",
            "max_length": 280,
        }
    )

    # Always ask main outcome with strong MCQ options.
    questions.append(
        {
            "id": "main_outcome",
            "type": "mcq_single",
            "required": True,
            "title": "Main Outcome",
            "prompt": "What is the main outcome you want from this blog?",
            "options": OUTCOME_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "main_outcome_custom",
        }
    )

    # Always ask CTA with hardcoded options.
    questions.append(
        {
            "id": "cta",
            "type": "mcq_single",
            "required": True,
            "title": "Call To Action",
            "prompt": "What should the reader do after reading the blog?",
            "options": CTA_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "cta_custom",
        }
    )

    # Ask depth as MCQ to control output quality.
    questions.append(
        {
            "id": "depth",
            "type": "mcq_single",
            "required": True,
            "title": "Depth",
            "prompt": "How detailed should this blog be?",
            "options": DEPTH_OPTIONS,
        }
    )

    # Always ask explicit yes/no about using saved company info.
    if company_context.get("available"):
        company_prompt = "Should we use your saved company info in this blog?"
    else:
        company_prompt = "We have limited saved info on your company. Should we use whatever is available?"
        notes.append("Saved company context is limited. You can type additional company details below(name,description...).")

    questions.append(
        {
            "id": "use_company_info",
            "type": "mcq_single",
            "required": True,
            "title": "Use Company Info",
            "prompt": company_prompt,
            "options": [
                {
                    "id": "yes",
                    "label": "Yes",
                    "description": "Use saved company context where relevant.",
                },
                {
                    "id": "no",
                    "label": "No",
                    "description": "Generate from blog answers only.",
                },
            ],
            "default": "yes" if company_context.get("available") else "no",
        }
    )

    # Always allow additional company context by typing.
    questions.append(
        {
            "id": "company_info_input",
            "type": "text_long",
            "required": False,
            "title": "Optional Additional Company Details",
            "prompt": "If you want to add extra company details (name, description, positioning) for this blog, type them here.",
            "placeholder": "Example: We are Acme, an AI startup helping enterprises reduce release cycle time.",
            "max_length": 500,
        }
    )

    return questions, notes


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        payload = parse_event(event)

        project_id = str(payload.get("project_id", "")).strip()
        session_id = str(payload.get("session_id", "")).strip()

        if not project_id:
            return response(400, {"success": False, "error": "Missing required field: project_id"})
        if not session_id:
            return response(400, {"success": False, "error": "Missing required field: session_id"})

        user_id = get_user_id_from_session(session_id)
        if not user_id:
            return response(401, {"success": False, "error": "Invalid session_id"})

        facts = load_facts(project_id)
        company_context = build_company_context(facts)
        prefilled_fields = build_prefilled_fields(facts, company_context)
        questions, notes = build_questions(facts, company_context, prefilled_fields)

        estimated_required = len([q for q in questions if q.get("required")])

        payload_out = {
            "success": True,
            "project_id": project_id,
            "session_id": session_id,
            "generated_at": datetime.utcnow().isoformat(),
            "company_context": {
                "available": company_context["available"],
                "context_level": company_context.get("context_level"),
                "context_score": company_context.get("context_score"),
                "summary_points": company_context["summary_points"],
                "use_company_info_question_included": True,
            },
            "prefilled_fields": prefilled_fields,
            "question_plan": {
                "goal": "minimum_questions_maximum_quality",
                "estimated_required_questions": estimated_required,
                "notes": notes,
            },
            "questions": questions,
            "frontend_guidance": {
                "show_prefill_review_first": True,
                "allow_other_custom_text": True,
                "allow_skip_for_optional": True,
            },
        }

        return response(200, payload_out)

    except ValueError as ve:
        return response(400, {"success": False, "error": str(ve)})
    except Exception as exc:
        return response(
            500,
            {
                "success": False,
                "error": "Failed to build blog intake questions",
                "details": str(exc),
            },
        )
