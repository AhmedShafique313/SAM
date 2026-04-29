import json
import os
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Attr, Key


dynamodb = boto3.resource("dynamodb")

USERS_TABLE_NAME = os.environ.get("USERS_TABLE", "users-table")
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")

users_table = dynamodb.Table(USERS_TABLE_NAME)
facts_table = dynamodb.Table(FACTS_TABLE_NAME)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}


# -------- Industry-standard MCQ option sets for a One-Pager --------

GOAL_OPTIONS = [
    {
        "id": "generate_leads",
        "label": "Generate leads",
        "description": "Capture interest from prospects and drive inbound inquiries.",
    },
    {
        "id": "book_demos",
        "label": "Book demos or sales calls",
        "description": "Move prospects into a scheduled conversation with sales.",
    },
    {
        "id": "sales_enablement",
        "label": "Sales enablement leave-behind",
        "description": "Support sales reps in meetings and follow-ups.",
    },
    {
        "id": "investor_pitch",
        "label": "Investor or partner pitch",
        "description": "Summarize the business for investors, partners, or stakeholders.",
    },
    {
        "id": "product_launch",
        "label": "Product or feature launch",
        "description": "Announce and explain a new product, feature, or update.",
    },
    {
        "id": "event_handout",
        "label": "Event or conference handout",
        "description": "Hand out at trade shows, events, or networking.",
    },
    {
        "id": "educate_prospects",
        "label": "Educate prospects",
        "description": "Explain a solution, category, or approach in a digestible way.",
    },
    {
        "id": "other",
        "label": "Other",
        "description": "Choose this if your goal is different.",
    },
]

AUDIENCE_OPTIONS = [
    {"id": "c_suite", "label": "C-suite / Executives"},
    {"id": "vp_director", "label": "VPs / Directors"},
    {"id": "managers", "label": "Managers / Team Leads"},
    {"id": "practitioners", "label": "Practitioners / Individual contributors"},
    {"id": "founders", "label": "Founders / Entrepreneurs"},
    {"id": "investors", "label": "Investors / Partners"},
    {"id": "smb_owners", "label": "Small business owners"},
    {"id": "end_consumers", "label": "End consumers (B2C)"},
    {"id": "other", "label": "Other"},
]

CTA_OPTIONS = [
    {"id": "book_demo", "label": "Book a demo"},
    {"id": "book_call", "label": "Book a strategy call"},
    {"id": "start_trial", "label": "Start free trial"},
    {"id": "contact_sales", "label": "Contact sales"},
    {"id": "request_quote", "label": "Request a quote"},
    {"id": "download_resource", "label": "Download a resource"},
    {"id": "visit_website", "label": "Visit website / Learn more"},
    {"id": "sign_up", "label": "Sign up / Get started"},
    {"id": "other", "label": "Other"},
]

TONE_OPTIONS = [
    {"id": "expert_strategic", "label": "Expert and strategic"},
    {"id": "friendly_simple", "label": "Friendly and simple"},
    {"id": "bold_opinionated", "label": "Bold and opinionated"},
    {"id": "formal_corporate", "label": "Formal and corporate"},
    {"id": "practical_no_fluff", "label": "Practical and no-fluff"},
    {"id": "inspirational", "label": "Inspirational and visionary"},
    {"id": "other", "label": "Other"},
]

PROOF_OPTIONS = [
    {"id": "customer_results", "label": "Customer results / case study metrics"},
    {"id": "testimonials", "label": "Customer testimonials / quotes"},
    {"id": "logos", "label": "Recognizable customer logos"},
    {"id": "awards", "label": "Awards and recognitions"},
    {"id": "certifications", "label": "Certifications / compliance badges"},
    {"id": "stats", "label": "Industry statistics or research"},
    {"id": "press", "label": "Press mentions"},
    {"id": "none", "label": "None / skip proof points"},
]

VISUAL_STYLE_OPTIONS = [
    {"id": "modern_minimal", "label": "Modern and minimal"},
    {"id": "bold_vibrant", "label": "Bold and vibrant"},
    {"id": "corporate_clean", "label": "Corporate and clean"},
    {"id": "playful_illustrated", "label": "Playful and illustrated"},
    {"id": "premium_editorial", "label": "Premium and editorial"},
    {"id": "data_heavy", "label": "Data-heavy / infographic style"},
    {"id": "match_brand", "label": "Match our existing brand"},
    {"id": "other", "label": "Other"},
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
            **CORS_HEADERS,
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
    key_fields = [
        ("company_name", "business.name"),
        ("company_description", "business.description_short"),
        ("industry", "business.industry"),
        ("core_offering", "product.core_offering"),
        ("value_prop", "product.value_proposition_short"),
        ("target_audience", "customer.primary_customer"),
        ("customer_problem", "customer.problems"),
        ("brand_tone", "brand.tone_personality"),
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
    if mapped.get("value_prop"):
        summary_points.append(f"Value proposition: {mapped['value_prop']}")
    if mapped.get("target_audience"):
        summary_points.append(f"Target audience: {mapped['target_audience']}")
    if mapped.get("customer_problem"):
        summary_points.append(f"Customer problem: {mapped['customer_problem']}")
    if mapped.get("brand_tone"):
        summary_points.append(f"Brand tone: {mapped['brand_tone']}")
    if mapped.get("proof_points"):
        summary_points.append(f"Proof points: {mapped['proof_points']}")

    core_signals = [
        mapped.get("company_name"),
        mapped.get("company_description"),
        mapped.get("core_offering"),
        mapped.get("target_audience"),
    ]
    support_signals = [
        mapped.get("value_prop"),
        mapped.get("customer_problem"),
        mapped.get("brand_tone"),
        mapped.get("proof_points"),
        mapped.get("industry"),
    ]

    core_count = sum(1 for x in core_signals if x)
    support_count = sum(1 for x in support_signals if x)
    score = (core_count * 2) + support_count

    if score >= 7:
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
    if any(word in lower for word in ["inspirational", "visionary", "aspirational"]):
        return "inspirational"
    return None


def build_prefilled_fields(facts: Dict[str, Dict[str, Any]], company_context: Dict[str, Any]) -> Dict[str, Any]:
    tone_raw = company_context["fields"].get("brand_tone")
    prefilled = {
        "audience_hint": company_context["fields"].get("target_audience"),
        "suggested_tone_option": normalize_tone_to_option(tone_raw),
        "suggested_tone_raw": tone_raw,
        "offer_hint": company_context["fields"].get("core_offering"),
        "value_prop_hint": company_context["fields"].get("value_prop"),
        "proof_points_hint": company_context["fields"].get("proof_points"),
    }
    return prefilled


def build_questions(
    facts: Dict[str, Dict[str, Any]],
    company_context: Dict[str, Any],
    prefilled: Dict[str, Any],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    questions: List[Dict[str, Any]] = []
    notes: List[str] = []

    # 1. Primary goal of the one-pager (MCQ)
    questions.append(
        {
            "id": "goal",
            "type": "mcq_single",
            "required": True,
            "title": "Primary Goal",
            "prompt": "What is the primary goal of this one-pager?",
            "options": GOAL_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "goal_custom",
        }
    )

    # 2. Target audience (MCQ)
    questions.append(
        {
            "id": "audience",
            "type": "mcq_single",
            "required": True,
            "title": "Target Audience",
            "prompt": "Who is the primary audience for this one-pager?",
            "options": AUDIENCE_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "audience_custom",
        }
    )

    # 3. Offer / what's being presented (short text - too specific to MCQ)
    questions.append(
        {
            "id": "offer",
            "type": "text_long",
            "required": True,
            "title": "The Offer",
            "prompt": "What specifically are you offering or presenting in this one-pager?",
            "placeholder": "Example: AI workflow optimization audit + 30-day pilot",
            "max_length": 280,
        }
    )

    # 4. Proof points (multi-select MCQ)
    questions.append(
        {
            "id": "proof_points",
            "type": "mcq_multi",
            "required": False,
            "title": "Proof Points to Include",
            "prompt": "Which types of proof points should we include? (select all that apply)",
            "options": PROOF_OPTIONS,
            "allow_custom_text": True,
            "custom_text_field": "proof_points_details",
            "custom_text_prompt": "Optionally add specific proof details (numbers, names, quotes).",
        }
    )

    # 5. Primary CTA (MCQ)
    questions.append(
        {
            "id": "cta",
            "type": "mcq_single",
            "required": True,
            "title": "Primary Call-to-Action",
            "prompt": "What should the reader do after seeing this one-pager?",
            "options": CTA_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "cta_custom",
        }
    )

    # 6. Tone (MCQ, prefilled from brand facts if available)
    tone_question = {
        "id": "tone",
        "type": "mcq_single",
        "required": False,
        "title": "Tone",
        "prompt": "What tone should the one-pager use?",
        "options": TONE_OPTIONS,
        "allow_custom_text_when": "other",
        "custom_text_field": "tone_custom",
    }
    if prefilled.get("suggested_tone_option"):
        tone_question["default"] = prefilled["suggested_tone_option"]
        tone_question["prefill_note"] = "Prefilled based on your saved brand tone."
    questions.append(tone_question)

    # 7. Visual style (MCQ)
    questions.append(
        {
            "id": "visual_style",
            "type": "mcq_single",
            "required": False,
            "title": "Visual Style",
            "prompt": "What visual style do you prefer for this one-pager?",
            "options": VISUAL_STYLE_OPTIONS,
            "allow_custom_text_when": "other",
            "custom_text_field": "visual_style_custom",
        }
    )

    # 8. Use saved company info (yes/no)
    if company_context.get("available"):
        company_prompt = "Should we use your saved company info in this one-pager?"
    else:
        company_prompt = "We have limited saved company info. Should we use whatever is available?"
        notes.append("Saved company context is limited. You can type additional company details below.")

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
                    "description": "Generate from one-pager answers only.",
                },
            ],
            "default": "yes" if company_context.get("available") else "no",
        }
    )

    # 9. Optional additional company context (free text)
    questions.append(
        {
            "id": "company_info_input",
            "type": "text_long",
            "required": False,
            "title": "Optional Additional Company Details",
            "prompt": "If you want to add extra company details for this one-pager, type them here.",
            "placeholder": "Example: We are Acme, an AI startup helping enterprises reduce release cycle time.",
            "max_length": 500,
        }
    )

    # 10. Optional constraints / must-include or must-avoid
    questions.append(
        {
            "id": "constraints",
            "type": "text_long",
            "required": False,
            "title": "Constraints or Must-Includes",
            "prompt": "Anything that must be included or avoided? (legal, compliance, jargon, etc.)",
            "placeholder": "Example: Do not make legal claims, avoid technical jargon",
            "max_length": 400,
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
            "module": "onepager",
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
            "next_endpoint_hint": "Submit answers to onepager-2 lambda in onepager_answers",
        }

        return response(200, payload_out)

    except ValueError as ve:
        return response(400, {"success": False, "error": str(ve)})
    except json.JSONDecodeError:
        return response(400, {"success": False, "error": "Invalid JSON payload"})
    except Exception as exc:
        return response(
            500,
            {
                "success": False,
                "error": "Failed to build one-pager intake questions",
                "details": str(exc),
            },
        )
