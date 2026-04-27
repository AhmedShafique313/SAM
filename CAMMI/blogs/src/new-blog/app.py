import json
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import boto3
from boto3.dynamodb.conditions import Attr, Key
from decimal import Decimal


dynamodb = boto3.resource("dynamodb")

USERS_TABLE_NAME = os.environ.get("USERS_TABLE", "users-table")
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")
BLOGS_TABLE_NAME = os.environ.get("BLOGS_TABLE_NAME", "blogs-table")

BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "us-east-1")
BEDROCK_MODEL_ID = os.environ.get(
    "BEDROCK_MODEL_ID",
    "us.anthropic.claude-sonnet-4-20250514-v1:0",
)

users_table = dynamodb.Table(USERS_TABLE_NAME)
facts_table = dynamodb.Table(FACTS_TABLE_NAME)
blogs_table = dynamodb.Table(BLOGS_TABLE_NAME)

bedrock_runtime = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)


OUTCOME_LABELS = {
    "trust_authority": "Build trust and authority",
    "qualified_leads": "Generate qualified leads",
    "discoverability": "Improve discoverability",
    "educate_market": "Educate the market",
    "launch_announcement": "Product or feature announcement",
    "thought_leadership": "Thought leadership",
    "conversion_demo": "Drive demos or conversions",
    "other": "Other",
}

CTA_LABELS = {
    "book_demo": "Book a demo",
    "start_trial": "Start free trial",
    "contact_sales": "Contact sales",
    "download_resource": "Download a resource",
    "subscribe_newsletter": "Subscribe to newsletter",
    "read_related": "Read a related article",
    "reply_comment": "Reply or engage",
    "other": "Other",
}

TONE_LABELS = {
    "expert_strategic": "Expert and strategic",
    "friendly_simple": "Friendly and simple",
    "bold_opinionated": "Bold and opinionated",
    "formal_corporate": "Formal and corporate",
    "practical_no_fluff": "Practical and no-fluff",
    "other": "Other",
}

DEPTH_LABELS = {
    "quick_read": "Quick read (700-900 words)",
    "standard": "Standard depth (1000-1400 words)",
    "deep_dive": "Deep dive (1600-2200 words)",
}

CORS_HEADERS = {}


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


def sanitize_for_dynamodb(obj: Any) -> Any:
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: sanitize_for_dynamodb(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize_for_dynamodb(i) for i in obj]
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


def map_or_custom(selected: Optional[str], custom_value: Optional[str], label_map: Dict[str, str]) -> Optional[str]:
    if not selected:
        return None
    if selected == "other":
        return (custom_value or "").strip() or None
    return label_map.get(selected, selected)


def invoke_bedrock_json(system_prompt: str, user_payload: Dict[str, Any]) -> Dict[str, Any]:
    body = json.dumps(
        {
            "anthropic_version": "bedrock-2023-05-31",
            "system": system_prompt + "\n\nReturn ONLY valid JSON.",
            "messages": [{"role": "user", "content": json.dumps(user_payload, default=str)}],
            "temperature": 0.3,
            "max_tokens": 4096,
        }
    )

    response_obj = bedrock_runtime.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=body,
        contentType="application/json",
        accept="application/json",
    )

    raw = json.loads(response_obj["body"].read())
    text = raw["content"][0]["text"].strip()

    while text.startswith("```"):
        if text.startswith("```json"):
            text = text[7:].strip()
        else:
            text = text[3:].strip()

    while text.endswith("```"):
        text = text[:-3].strip()

    return json.loads(text)


def build_effective_brief(
    facts: Dict[str, Dict[str, Any]],
    answers: Dict[str, Any],
) -> Dict[str, Any]:
    use_company_mode = str(answers.get("use_company_info", "yes")).strip().lower() or "yes"

    if use_company_mode in ["yes", "yes_use_all", "yes_use_no_name", "true", "1"]:
        use_company_info = True
    else:
        use_company_info = False

    include_company_name = use_company_info

    topic = str(answers.get("blog_topic", "")).strip()
    outcome = map_or_custom(
        str(answers.get("main_outcome", "")).strip() or None,
        str(answers.get("main_outcome_custom", "")).strip() or None,
        OUTCOME_LABELS,
    )
    cta = map_or_custom(
        str(answers.get("cta", "")).strip() or None,
        str(answers.get("cta_custom", "")).strip() or None,
        CTA_LABELS,
    )
    tone = map_or_custom(
        str(answers.get("tone", "")).strip() or None,
        str(answers.get("tone_custom", "")).strip() or None,
        TONE_LABELS,
    )
    depth = DEPTH_LABELS.get(str(answers.get("depth", "")).strip(), None)

    audience = str(answers.get("audience", "")).strip() or get_fact_value(facts, "customer.primary_customer")

    company_context_input = str(answers.get("company_info_input", "")).strip()

    company_context = {
        "company_name": get_fact_value(facts, "business.name") if include_company_name else None,
        "company_description": get_fact_value(facts, "business.description_short") if use_company_info else None,
        "industry": get_fact_value(facts, "business.industry") if use_company_info else None,
        "core_offering": get_fact_value(facts, "product.core_offering") if use_company_info else None,
        "value_proposition": get_fact_value(facts, "product.value_proposition_short") if use_company_info else None,
        "customer_problem": get_fact_value(facts, "customer.problems") if use_company_info else None,
        "brand_tone_from_facts": get_fact_value(facts, "brand.tone_personality") if use_company_info else None,
        "key_messages": get_fact_value(facts, "brand.key_messages") if use_company_info else None,
        "proof_points": get_fact_value(facts, "assets.brag_points") if use_company_info else None,
        "case_studies": get_fact_value(facts, "assets.case_studies") if use_company_info else None,
        "quotes": get_fact_value(facts, "assets.quotes") if use_company_info else None,
    }

    if company_context_input:
        company_context["manual_company_context"] = company_context_input

    if not tone:
        tone = company_context.get("brand_tone_from_facts")

    brief = {
        "topic": topic,
        "main_outcome": outcome,
        "cta": cta,
        "tone": tone,
        "depth": depth,
        "audience": audience,
        "use_company_info": use_company_info,
        "include_company_name": include_company_name,
        "company_context": company_context,
    }

    return brief


def validate_brief(brief: Dict[str, Any]) -> List[str]:
    missing = []
    if not brief.get("topic"):
        missing.append("blog_topic")
    if not brief.get("main_outcome"):
        missing.append("main_outcome")
    if not brief.get("cta"):
        missing.append("cta")
    if not brief.get("depth"):
        missing.append("depth")
    return missing


def generate_blog_json(brief: Dict[str, Any], additional_payload: Dict[str, Any]) -> Dict[str, Any]:
    system_prompt = """You are an elite marketing blog writer for business users.

Write exactly ONE blog post as strict JSON with this schema:
{
  "title": "...",
  "slug": "...",
  "meta": {
    "meta_title": "...",
    "meta_description": "...",
    "excerpt": "...",
    "estimated_read_time": "...",
    "target_outcome": "...",
    "target_audience": "...",
    "tone": "...",
    "cta": "..."
  },
  "outline": [
    {"heading": "...", "purpose": "..."}
  ],
  "blog": {
    "introduction": "...",
    "sections": [
      {
        "heading": "...",
        "content": "...",
        "key_points": ["...", "..."]
      }
    ],
    "conclusion": "...",
    "cta_block": "..."
  },
  "distribution": {
    "linkedin_caption": "...",
    "hashtags": ["..."]
  },
  "quality_checks": {
    "claims_to_verify": ["..."],
    "disclaimers": ["..."]
  }
}

Rules:
1. Keep hashtags only in distribution.hashtags, never in blog body.
2. Do not invent hard numbers or fake data sources.
3. Keep tone consistent with the brief.
4. Make the blog genuinely useful and non-generic.
5. Return valid JSON only.
"""

    payload = {
        "blog_brief": brief,
        "seo_payload_from_client": additional_payload.get("seo_payload"),
        "keyword_bundle_from_client": additional_payload.get("keyword_bundle"),
        "extra_instructions": additional_payload.get("extra_instructions"),
    }

    generated = invoke_bedrock_json(system_prompt, payload)
    return generated


def escape_html(text: str) -> str:
    """Minimal HTML escaping for plain text values."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def blog_json_to_html(blog_output: Dict[str, Any]) -> str:
    """
    Convert the structured blog_output JSON into HTML.
    """
    blog = blog_output.get("blog", {})
    parts: List[str] = []

    introduction = (blog.get("introduction") or "").strip()
    if introduction:
        parts.append(f"<p>{escape_html(introduction)}</p>")

    for section in blog.get("sections", []):
        heading = (section.get("heading") or "").strip()
        content = (section.get("content") or "").strip()
        key_points: List[str] = section.get("key_points") or []

        if heading:
            parts.append(f"\n<h2>{escape_html(heading)}</h2>")
        if content:
            parts.append(f"<p>{escape_html(content)}</p>")
        if key_points:
            li_items = "\n  ".join(
                f"<li>{escape_html(str(kp))}</li>" for kp in key_points if str(kp).strip()
            )
            if li_items:
                parts.append(f"<ul>\n  {li_items}\n</ul>")

    conclusion = (blog.get("conclusion") or "").strip()
    if conclusion:
        parts.append(f"\n<h2>Conclusion</h2>")
        parts.append(f"<p>{escape_html(conclusion)}</p>")

    cta_block = (blog.get("cta_block") or "").strip()
    if cta_block:
        parts.append(f"\n<blockquote>{escape_html(cta_block)}</blockquote>")

    return "\n".join(parts)


def save_blog_record(
    project_id: str,
    blog_id: str,
    user_id: str,
    brief: Dict[str, Any],
    blog_output: Dict[str, Any],
    blog_html: str,
    generation_metadata: Dict[str, Any],
) -> None:
    now = datetime.utcnow().isoformat()
    title = str(blog_output.get("title", "")).strip()
    slug = str(blog_output.get("slug", "")).strip()

    item = {
        "project_id": project_id,
        "blog_id": blog_id,
        "user_id": user_id,
        "title": title,
        "slug": slug,
        "status": "generated",
        "blog_brief": brief,
        "blog_output": blog_output,
        "blog_html": blog_html,
        "generation_metadata": generation_metadata,
        "created_at": now,
        "updated_at": now,
    }

    blogs_table.put_item(
        Item=sanitize_for_dynamodb(item),
        ConditionExpression="attribute_not_exists(project_id) AND attribute_not_exists(blog_id)",
    )


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        payload = parse_event(event)

        project_id = str(payload.get("project_id", "")).strip()
        session_id = str(payload.get("session_id", "")).strip()
        answers = payload.get("blog_answers", {})

        if not project_id:
            return response(400, {"success": False, "error": "Missing required field: project_id"})
        if not session_id:
            return response(400, {"success": False, "error": "Missing required field: session_id"})
        if not isinstance(answers, dict):
            return response(400, {"success": False, "error": "blog_answers must be an object"})

        user_id = get_user_id_from_session(session_id)
        if not user_id:
            return response(401, {"success": False, "error": "Invalid session_id"})

        facts = load_facts(project_id)
        brief = build_effective_brief(facts, answers)
        missing = validate_brief(brief)

        if missing:
            return response(
                400,
                {
                    "success": False,
                    "error": "Missing required blog fields",
                    "missing_fields": missing,
                    "message": "Please complete missing required inputs from blog intake before generation.",
                },
            )

        blog_json = generate_blog_json(brief, payload)
        blog_html = blog_json_to_html(blog_json)
        blog_id = str(uuid.uuid4())

        facts_used = []
        if brief.get("use_company_info"):
            for fid in [
                "business.name",
                "business.description_short",
                "business.industry",
                "product.core_offering",
                "product.value_proposition_short",
                "customer.primary_customer",
                "customer.problems",
                "brand.tone_personality",
                "brand.key_messages",
                "assets.brag_points",
                "assets.case_studies",
                "assets.quotes",
            ]:
                if get_fact_value(facts, fid):
                    facts_used.append(fid)

        generation_metadata = {
            "model_id": BEDROCK_MODEL_ID,
            "region": BEDROCK_REGION,
            "one_blog_only": True,
            "used_company_info": brief.get("use_company_info"),
            "include_company_name": brief.get("include_company_name"),
            "facts_used": facts_used,
        }

        save_blog_record(
            project_id=project_id,
            blog_id=blog_id,
            user_id=user_id,
            brief=brief,
            blog_output=blog_json,
            blog_html=blog_html,
            generation_metadata=generation_metadata,
        )

        result_payload = {
            "success": True,
            "project_id": project_id,
            "blog_id": blog_id,
            "session_id": session_id,
            "generated_at": datetime.utcnow().isoformat(),
            "blog_brief": brief,
            "blog_output": blog_json,
            "blog_html": blog_html,
            "generation_metadata": generation_metadata,
            "storage": {
                "table": BLOGS_TABLE_NAME,
                "saved": True,
            },
        }

        return response(200, result_payload)

    except ValueError as ve:
        return response(400, {"success": False, "error": str(ve)})
    except json.JSONDecodeError:
        return response(400, {"success": False, "error": "Invalid JSON payload"})
    except Exception as exc:
        return response(
            500,
            {
                "success": False,
                "error": "Blog generation failed",
                "details": str(exc),
            },
        )
