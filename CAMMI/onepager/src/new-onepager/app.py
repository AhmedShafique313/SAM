import base64
import json
import logging
import os
import random
import re
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

import boto3
from boto3.dynamodb.conditions import Attr, Key


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    logger.addHandler(handler)


dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")

USERS_TABLE_NAME = os.environ.get("USERS_TABLE", "users-table")
FACTS_TABLE_NAME = os.environ.get("FACTS_TABLE_NAME", "facts-table")
ONEPAGER_TABLE_NAME = os.environ.get("ONEPAGER_TABLE_NAME", "onepager-table")
ONEPAGER_SORT_KEY_NAME = os.environ.get("ONEPAGER_SORT_KEY_NAME", "onepager_id")

BEDROCK_REGION = "us-east-1"
BEDROCK_TEXT_MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
BEDROCK_RERANK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

ASSETS_BUCKET = os.environ.get("ASSETS_BUCKET", "cammi-devprod")
ASSETS_PREFIX = os.environ.get("ASSETS_PREFIX", "imagesonepager")

UNSPLASH_ACCESS_KEY = os.environ.get("UNSPLASH_ACCESS_KEY", "")
PEXELS_API_KEY = os.environ.get("PEXELS_API_KEY", "")
PIXABAY_API_KEY = os.environ.get("PIXABAY_API_KEY", "")

IMAGE_CANDIDATES_PER_PROVIDER = 10
MIN_IMAGE_RELEVANCE_SCORE = 3.5
LLM_RERANK_UNCERTAIN_THRESHOLD = 6.0
ENABLE_LLM_RERANK = True

HTTP_USER_AGENT = "Mozilla/5.0 (compatible; OnepagerBot/1.0; +https://cammi.ai)"

users_table = dynamodb.Table(USERS_TABLE_NAME)
facts_table = dynamodb.Table(FACTS_TABLE_NAME)
onepager_table = dynamodb.Table(ONEPAGER_TABLE_NAME)

bedrock_runtime = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

CORS_HEADERS = {
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}


_MAX_QUERY_WORDS = 8
_MIN_QUERY_WORDS = 4
_PROVIDER_QUERY_CHAR_LIMIT = {
    "pixabay": 100,
    "unsplash": 200,
    "pexels": 200,
}

_STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "they", "their", "into",
    "over", "under", "about", "using", "while", "which", "some", "show", "showing",
    "image", "photo", "scene", "style", "looking", "wearing", "holding", "been",
    "have", "has", "was", "were", "will", "are", "not", "but", "our", "your",
    "his", "her", "its", "them", "who", "what", "when", "where", "how",
    "on", "in", "at", "to", "of", "a", "an",
}

_GENERIC_STOCK_TERMS = {
    "business", "office", "meeting", "team", "corporate", "workspace", "company",
    "people", "laptop", "conference", "employee", "workers", "colleagues", "desk",
    "professional", "working", "work",
    "abstract", "representation", "concept", "illustration", "colorful", "modern",
    "digital", "minimal", "simple", "clean", "elegant", "vibrant", "beautiful",
    "creative", "background", "design", "style", "line", "drawing", "art",
    "graphic", "artistic", "detailed",
}

MAX_BASE64_EMBED_BYTES = int(os.environ.get("MAX_BASE64_EMBED_BYTES", 600_000))


# ---------------------------------------------------------------------------
# Request / response helpers
# ---------------------------------------------------------------------------
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
            logger.warning(f"[auth] No user found for session_id={session_id}")
            return None
        return items[0].get("id")
    except Exception as e:
        logger.exception(f"[auth] Error looking up session_id={session_id}: {e}")
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

    logger.info(f"[facts] Loaded {len(facts)} facts for project_id={project_id}")
    return facts


def get_fact_value(facts: Dict[str, Dict[str, Any]], key: str) -> Optional[str]:
    raw = facts.get(key, {}).get("value")
    if raw is None:
        return None
    text = str(raw).strip()
    return text if text else None


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

    start = time.time()
    response_obj = bedrock_runtime.invoke_model(
        modelId=BEDROCK_TEXT_MODEL_ID,
        body=body,
        contentType="application/json",
        accept="application/json",
    )
    duration = time.time() - start
    logger.info(f"[bedrock_text] model={BEDROCK_TEXT_MODEL_ID} duration={duration:.2f}s")

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


def build_onepager_brief(facts: Dict[str, Dict[str, Any]], answers: Dict[str, Any]) -> Dict[str, Any]:
    use_company_mode = str(answers.get("use_company_info", "yes")).strip().lower() or "yes"
    use_company_info = use_company_mode in ["yes", "true", "1"]

    brief = {
        "goal": str(answers.get("goal", "")).strip(),
        "audience": str(answers.get("audience", "")).strip() or get_fact_value(facts, "customer.primary_customer"),
        "offer": str(answers.get("offer", "")).strip() or get_fact_value(facts, "product.core_offering"),
        "proof_points": str(answers.get("proof_points", "")).strip() or get_fact_value(facts, "assets.brag_points"),
        "objections": str(answers.get("objections", "")).strip() or get_fact_value(facts, "customer.objections"),
        "cta": str(answers.get("cta", "")).strip(),
        "tone": str(answers.get("tone", "")).strip() or get_fact_value(facts, "brand.tone_personality"),
        "constraints": str(answers.get("constraints", "")).strip(),
        "visual_preferences": str(answers.get("visual_preferences", "")).strip(),
        "use_company_info": use_company_info,
        "company_context": {
            "company_name": get_fact_value(facts, "business.name") if use_company_info else None,
            "company_description": get_fact_value(facts, "business.description_short") if use_company_info else None,
            "industry": get_fact_value(facts, "business.industry") if use_company_info else None,
            "value_proposition": get_fact_value(facts, "product.value_proposition_short") if use_company_info else None,
            "manual_company_context": str(answers.get("company_info_input", "")).strip() or None,
        },
    }

    return brief


def validate_brief(brief: Dict[str, Any]) -> List[str]:
    missing = []
    if not brief.get("goal"):
        missing.append("goal")
    if not brief.get("offer"):
        missing.append("offer")
    if not brief.get("cta"):
        missing.append("cta")
    return missing


def generate_onepager_json(brief: Dict[str, Any], additional_payload: Dict[str, Any]) -> Dict[str, Any]:
    system_prompt = """You are an expert conversion copywriter and one-page website strategist.

Return strict JSON with this schema:
{
  "title": "...",
  "slug": "...",
  "meta": {
    "meta_title": "...",
    "meta_description": "..."
  },
  "onepager": {
    "hero": {
      "headline": "...",
      "subheadline": "...",
      "primary_cta": "...",
      "secondary_cta": "..."
    },
    "sections": [
      {
        "id": "problem",
        "heading": "...",
        "content": "...",
        "bullets": ["...", "..."]
      }
    ],
    "faq": [
      {"q": "...", "a": "..."}
    ]
  },
  "visual_plan": [
    {
      "visual_id": "hero_visual",
      "type": "photo|illustration|abstract|icon",
      "placement": "hero|section_id",
      "query": "specific stock photo search query",
      "style_prompt": "detailed AI image generation prompt",
      "icon_name": "optional icon token",
      "semantic_description": "one sentence describing exactly what the ideal image looks like, used for relevance matching"
    }
  ]
}

COPY RULES:
1. Copy must be concrete, benefit-led, and conversion-focused.
2. Use the audience's actual language, not marketing jargon.
3. Every section must tie back to the brief's goal and CTA.

VISUAL PLAN RULES (CRITICAL - read carefully):
1. Each visual MUST be tied to a specific section via "placement" and reflect that section's actual content.
2. "query" is for stock photo APIs. It MUST be EXACTLY 4-8 words, keyword-dense. Follow this formula:
   [audience/subject-noun] + [specific action OR object] + [industry/setting noun]
   - BAD: "minimal line drawing newsletter email startup founder reading simple illustration"
   - BAD: "community startup founders network minimal connected dots abstract representation"
   - GOOD: "founder reading email laptop cafe"
   - GOOD: "warehouse manager tablet logistics dashboard"
   - GOOD: "entrepreneur writing notebook coffee shop"
3. "query" MUST name the SPECIFIC audience noun from the brief (e.g. "founder", "manager", "nurse"). Never use abstract words like "community", "network", "concept", "abstract", "representation".
4. BANNED query terms: handshake, thumbs up, lightbulb, puzzle pieces, rocket, chess, bullseye, generic "team", "success", "happy", "celebrating", "abstract", "representation", "connected dots", "concept".
5. "style_prompt" may be longer and is only used as fallback. Include concrete subject, setting, lighting.
6. "semantic_description" is a plain-English sentence describing the ideal image.
7. Generate 3-6 visuals total: one hero, and one per major section. NEVER generate visuals for FAQ sections.
8. If a section is abstract (e.g., pricing, guarantees), prefer type "icon" over "photo".
9. If "visual_preferences" is present in the brief, apply it but do NOT add its words directly to "query" (they are style words, not search keywords).

OUTPUT: Return valid JSON only. No commentary, no markdown fences.
"""

    payload = {
        "onepager_brief": brief,
        "seo_payload_from_client": additional_payload.get("seo_payload"),
        "keyword_bundle_from_client": additional_payload.get("keyword_bundle"),
        "extra_instructions": additional_payload.get("extra_instructions"),
    }

    return invoke_bedrock_json(system_prompt, payload)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
def _merge_headers(extra: Optional[Dict[str, str]]) -> Dict[str, str]:
    base = {
        "User-Agent": HTTP_USER_AGENT,
        "Accept": "application/json",
    }
    if extra:
        base.update(extra)
    return base


def _host_of(url: str) -> str:
    try:
        return url.split("/")[2]
    except Exception:
        return "unknown"


def _http_get_json(url: str, headers: Optional[Dict[str, str]] = None, tag: str = "http") -> Optional[Dict[str, Any]]:
    req = Request(url, headers=_merge_headers(headers), method="GET")
    start = time.time()
    try:
        with urlopen(req, timeout=10) as resp:
            status = resp.status
            raw = resp.read().decode("utf-8")
            duration = time.time() - start
            logger.info(f"[{tag}] GET status={status} duration={duration:.2f}s host={_host_of(url)}")
            return json.loads(raw)
    except HTTPError as e:
        body_preview = ""
        try:
            body_preview = e.read().decode("utf-8")[:400]
        except Exception:
            pass
        logger.error(f"[{tag}] HTTPError status={e.code} reason={e.reason} host={_host_of(url)} body={body_preview}")
        return None
    except URLError as e:
        logger.error(f"[{tag}] URLError reason={e.reason} host={_host_of(url)}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"[{tag}] JSONDecodeError host={_host_of(url)} err={e}")
        return None
    except Exception as e:
        logger.exception(f"[{tag}] Unexpected error host={_host_of(url)}: {e}")
        return None


def _http_get_bytes(url: str, headers: Optional[Dict[str, str]] = None, tag: str = "http_bytes") -> Optional[bytes]:
    req = Request(url, headers=_merge_headers(headers), method="GET")
    start = time.time()
    try:
        with urlopen(req, timeout=15) as resp:
            status = resp.status
            data = resp.read()
            duration = time.time() - start
            logger.info(f"[{tag}] GET bytes status={status} size={len(data)} duration={duration:.2f}s host={_host_of(url)}")
            return data
    except HTTPError as e:
        logger.error(f"[{tag}] HTTPError status={e.code} reason={e.reason} host={_host_of(url)}")
        return None
    except URLError as e:
        logger.error(f"[{tag}] URLError reason={e.reason} host={_host_of(url)}")
        return None
    except Exception as e:
        logger.exception(f"[{tag}] Unexpected error host={_host_of(url)}: {e}")
        return None


# ---------------------------------------------------------------------------
# Tokenization & query building
# ---------------------------------------------------------------------------
def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s-]", " ", text)
    return [w for w in text.split() if len(w) > 2 and w not in _STOPWORDS]


def _bigrams(tokens: List[str]) -> List[str]:
    return [f"{tokens[i]} {tokens[i+1]}" for i in range(len(tokens) - 1)]


def _enforce_query_length(query: str) -> str:
    tokens = _tokenize(query)
    if not tokens:
        return query.strip()
    specific = [t for t in tokens if t not in _GENERIC_STOCK_TERMS]
    final = specific if len(specific) >= _MIN_QUERY_WORDS else tokens
    return " ".join(final[:_MAX_QUERY_WORDS])


def _inject_brief_anchors(query: str, brief: Dict[str, Any]) -> str:
    q_tokens = set(_tokenize(query))

    audience_tokens = _tokenize(str(brief.get("audience", "") or ""))
    offer_tokens = _tokenize(str(brief.get("offer", "") or ""))
    industry_tokens = _tokenize(str(brief.get("company_context", {}).get("industry", "") or ""))

    candidates = []
    for t in audience_tokens + offer_tokens + industry_tokens:
        if t not in _GENERIC_STOCK_TERMS and t not in candidates:
            candidates.append(t)

    if any(a in q_tokens for a in candidates):
        return query

    if candidates:
        return _enforce_query_length(f"{candidates[0]} {query}")
    return query


def _build_retry_query(query: str, semantic_desc: str, visual_preferences: str = "") -> str:
    q_tokens = _tokenize(query)
    s_tokens = _tokenize(semantic_desc)

    semantic_specific = [t for t in s_tokens if t not in _GENERIC_STOCK_TERMS and t not in q_tokens]
    query_specific = [t for t in q_tokens if t not in _GENERIC_STOCK_TERMS]

    combined: List[str] = []
    for t in semantic_specific[:3] + query_specific:
        if t not in combined:
            combined.append(t)

    if len(combined) < _MIN_QUERY_WORDS:
        for t in q_tokens + s_tokens:
            if t not in combined:
                combined.append(t)

    return " ".join(combined[:_MAX_QUERY_WORDS]) if combined else query


def _truncate_query_for_provider(query: str, provider: str) -> str:
    limit = _PROVIDER_QUERY_CHAR_LIMIT.get(provider, 200)
    normalized = " ".join(str(query or "").split())
    if len(normalized) <= limit:
        return normalized

    words = normalized.split()
    out: List[str] = []
    total = 0
    for w in words:
        add = len(w) if not out else len(w) + 1
        if total + add > limit:
            break
        out.append(w)
        total += add

    return " ".join(out) if out else normalized[:limit]


# ---------------------------------------------------------------------------
# Stock provider fetchers
# ---------------------------------------------------------------------------
def fetch_unsplash_candidates(query: str, count: int = 3) -> List[Dict[str, Any]]:
    if not UNSPLASH_ACCESS_KEY:
        logger.info("[unsplash] skipped: UNSPLASH_ACCESS_KEY not set")
        return []

    safe_query = _truncate_query_for_provider(query, "unsplash")
    params = urlencode({"query": safe_query, "per_page": count, "orientation": "landscape"})
    url = f"https://api.unsplash.com/search/photos?{params}"
    data = _http_get_json(url, headers={"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}, tag="unsplash")
    if not data:
        return []

    results = data.get("results") or []
    logger.info(f"[unsplash] query='{safe_query[:80]}' results_total={data.get('total', '?')} returned={len(results)}")

    out = []
    for r in results[:count]:
        url_ = r.get("urls", {}).get("regular")
        if not url_:
            continue
        out.append({
            "provider": "unsplash",
            "source_url": url_,
            "source_page": r.get("links", {}).get("html"),
            "attribution": r.get("user", {}).get("name"),
            "description": (r.get("description") or r.get("alt_description") or "").strip(),
            "query_used": safe_query,
        })
    return out


def fetch_pexels_candidates(query: str, count: int = 3) -> List[Dict[str, Any]]:
    if not PEXELS_API_KEY:
        logger.info("[pexels] skipped: PEXELS_API_KEY not set")
        return []

    safe_query = _truncate_query_for_provider(query, "pexels")
    params = urlencode({"query": safe_query, "per_page": count, "orientation": "landscape"})
    url = f"https://api.pexels.com/v1/search?{params}"
    data = _http_get_json(url, headers={"Authorization": PEXELS_API_KEY}, tag="pexels")
    if not data:
        return []

    photos = data.get("photos") or []
    logger.info(f"[pexels] query='{safe_query[:80]}' total_results={data.get('total_results', '?')} returned={len(photos)}")

    out = []
    for p in photos[:count]:
        url_ = p.get("src", {}).get("large")
        if not url_:
            continue
        out.append({
            "provider": "pexels",
            "source_url": url_,
            "source_page": p.get("url"),
            "attribution": p.get("photographer"),
            "description": (p.get("alt") or "").strip(),
            "query_used": safe_query,
        })
    return out


def fetch_pixabay_candidates(query: str, count: int = 3) -> List[Dict[str, Any]]:
    if not PIXABAY_API_KEY:
        logger.info("[pixabay] skipped: PIXABAY_API_KEY not set")
        return []

    safe_query = _truncate_query_for_provider(query, "pixabay")
    params = urlencode({"key": PIXABAY_API_KEY, "q": safe_query, "image_type": "photo", "per_page": count})
    url = f"https://pixabay.com/api/?{params}"
    data = _http_get_json(url, tag="pixabay")
    if not data:
        return []

    hits = data.get("hits") or []
    logger.info(f"[pixabay] query='{safe_query[:80]}' total={data.get('total', '?')} returned={len(hits)}")

    out = []
    for h in hits[:count]:
        url_ = h.get("webformatURL")
        if not url_:
            continue
        out.append({
            "provider": "pixabay",
            "source_url": url_,
            "source_page": h.get("pageURL"),
            "attribution": h.get("user"),
            "description": " ".join((h.get("tags") or "").split(",")).strip(),
            "query_used": safe_query,
        })
    return out


def gather_image_candidates(query: str, count_per_provider: int = IMAGE_CANDIDATES_PER_PROVIDER) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []

    active_fetchers = []
    if UNSPLASH_ACCESS_KEY:
        active_fetchers.append(("unsplash", fetch_unsplash_candidates))
    if PEXELS_API_KEY:
        active_fetchers.append(("pexels", fetch_pexels_candidates))
    if PIXABAY_API_KEY:
        active_fetchers.append(("pixabay", fetch_pixabay_candidates))

    if not active_fetchers:
        logger.error("[candidates] no stock providers active — check env vars")
        return []

    logger.info(
        f"[candidates] fetching query='{query[:100]}' count_per_provider={count_per_provider} "
        f"providers={[p[0] for p in active_fetchers]}"
    )
    start = time.time()

    with ThreadPoolExecutor(max_workers=len(active_fetchers)) as executor:
        futures = {executor.submit(fn, query, count_per_provider): name for name, fn in active_fetchers}
        for f, name in futures.items():
            try:
                result = f.result()
                logger.info(f"[candidates] provider={name} returned={len(result or [])}")
                candidates.extend(result or [])
            except Exception as e:
                logger.exception(f"[candidates] provider={name} raised: {e}")

    duration = time.time() - start

    seen = set()
    unique = []
    for c in candidates:
        u = c.get("source_url")
        if u and u not in seen:
            seen.add(u)
            unique.append(c)

    logger.info(f"[candidates] total={len(candidates)} unique={len(unique)} duration={duration:.2f}s")
    return unique


# ---------------------------------------------------------------------------
# Ranker
# ---------------------------------------------------------------------------
def _score_candidate(
    c: Dict[str, Any],
    q_unigrams: set,
    q_bigrams: set,
    s_unigrams: set,
    s_bigrams: set,
    domain_tokens: set,
) -> float:
    desc = (c.get("description") or "").strip()
    d_tokens_raw = _tokenize(desc)
    d_unigrams = set(d_tokens_raw)

    if not d_unigrams:
        return 0.0

    if domain_tokens and not (domain_tokens & d_unigrams):
        return 0.0

    seen = set()
    d_tokens_ordered_unique: List[str] = []
    for t in d_tokens_raw:
        if t not in seen:
            seen.add(t)
            d_tokens_ordered_unique.append(t)
    d_bigrams = set(_bigrams(d_tokens_ordered_unique))

    uni_overlap = q_unigrams & d_unigrams
    specific_uni_overlap = uni_overlap - _GENERIC_STOCK_TERMS
    generic_uni_overlap = uni_overlap & _GENERIC_STOCK_TERMS

    score = 0.0
    score += 2.0 * len(specific_uni_overlap)
    score += 0.2 * len(generic_uni_overlap)
    score += 3.0 * len(q_bigrams & d_bigrams)
    score += 0.5 * len(s_unigrams & d_unigrams - _GENERIC_STOCK_TERMS)
    score += 1.0 * len(s_bigrams & d_bigrams)

    if len(d_unigrams) > 20 and len(specific_uni_overlap) <= 1:
        score -= 2.0

    return score


def pick_best_candidate(
    candidates: List[Dict[str, Any]],
    query: str,
    semantic_desc: str = "",
) -> Dict[str, Any]:
    if not candidates:
        return {"candidate": None, "score": 0.0, "tier": "none", "scores_log": [], "ranked": []}

    q_tokens = _tokenize(query)
    s_tokens = _tokenize(semantic_desc)

    if not q_tokens and not s_tokens:
        logger.info("[ranker] no usable target words — picking first")
        return {"candidate": candidates[0], "score": 0.0, "tier": "first_no_tokens", "scores_log": [], "ranked": []}

    q_unigrams = set(q_tokens)
    q_bigrams = set(_bigrams(q_tokens))
    s_unigrams = set(s_tokens) - q_unigrams
    s_bigrams = set(_bigrams(s_tokens)) - q_bigrams

    domain_tokens = (q_unigrams | s_unigrams) - _GENERIC_STOCK_TERMS

    scored: List[Tuple[Dict[str, Any], float]] = [
        (c, _score_candidate(c, q_unigrams, q_bigrams, s_unigrams, s_bigrams, domain_tokens))
        for c in candidates
    ]
    scored.sort(key=lambda x: -x[1])

    scores_log = [
        (c.get("provider"), round(s, 2), (c.get("description") or "")[:60])
        for c, s in scored
    ]

    premium = [(c, s) for c, s in scored if c.get("provider") in ("unsplash", "pexels")]
    fallback = [(c, s) for c, s in scored if c.get("provider") == "pixabay"]

    best_premium = premium[0] if premium else (None, -1.0)
    best_fallback = fallback[0] if fallback else (None, -1.0)

    if best_premium[0] and best_premium[1] >= MIN_IMAGE_RELEVANCE_SCORE:
        chosen, chosen_score = best_premium
        tier = "premium"
    elif best_fallback[0] and best_fallback[1] > best_premium[1]:
        chosen, chosen_score = best_fallback
        tier = "fallback"
    elif best_premium[0]:
        chosen, chosen_score = best_premium
        tier = "premium_below_threshold"
    else:
        chosen, chosen_score = scored[0]
        tier = "first_resort_below_threshold"

    logger.info(
        f"[ranker] q_unigrams={len(q_unigrams)} q_bigrams={len(q_bigrams)} "
        f"domain_tokens={sorted(domain_tokens)[:8]} "
        f"winner_score={chosen_score:.2f} tier={tier} "
        f"winner_provider={chosen.get('provider') if chosen else None} "
        f"min_score={MIN_IMAGE_RELEVANCE_SCORE}"
    )
    return {
        "candidate": chosen,
        "score": float(chosen_score),
        "tier": tier,
        "scores_log": scores_log,
        "ranked": scored,
    }


# ---------------------------------------------------------------------------
# LLM rerank
# ---------------------------------------------------------------------------
def llm_rerank_top_candidates(
    top_candidates: List[Dict[str, Any]],
    semantic_desc: str,
    brief: Dict[str, Any],
) -> Optional[int]:
    if not top_candidates:
        return None

    choices_text = "\n".join(
        f"{i}. [{c.get('provider')}] {(c.get('description') or '(no description)')[:200]}"
        for i, c in enumerate(top_candidates)
    )

    system = (
        "You are an image relevance judge. Given a target description and a list of "
        "candidate image descriptions, return ONLY a JSON object: "
        '{"best_index": <int>, "reason": ""}. '
        "Pick the index whose description most literally matches the target subject, "
        "setting, and action. Reject candidates that are metaphorical, abstract, or "
        "feature the wrong subject (e.g. teenagers when target says founders)."
    )

    user_payload = {
        "target_description": semantic_desc,
        "brief_audience": brief.get("audience"),
        "brief_offer": brief.get("offer"),
        "candidates": choices_text,
    }

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "system": system + "\n\nReturn ONLY valid JSON.",
        "messages": [{"role": "user", "content": json.dumps(user_payload, default=str)}],
        "temperature": 0.0,
        "max_tokens": 200,
    })

    start = time.time()
    try:
        resp = bedrock_runtime.invoke_model(
            modelId=BEDROCK_RERANK_MODEL_ID,
            body=body,
            contentType="application/json",
            accept="application/json",
        )
    except Exception as e:
        logger.warning(f"[llm_rerank] invoke failed: {e}")
        return None

    duration = time.time() - start

    try:
        raw = json.loads(resp["body"].read())
        text = raw["content"][0]["text"].strip()
        while text.startswith("```"):
            if text.startswith("```json"):
                text = text[7:].strip()
            else:
                text = text[3:].strip()
        while text.endswith("```"):
            text = text[:-3].strip()
        result = json.loads(text)
        best_index = int(result.get("best_index", 0))
        logger.info(f"[llm_rerank] best_index={best_index} reason={result.get('reason', '')[:80]} duration={duration:.2f}s")
        if 0 <= best_index < len(top_candidates):
            return best_index
        return None
    except Exception as e:
        logger.warning(f"[llm_rerank] parse failed: {e}")
        return None


# ---------------------------------------------------------------------------
# Image selection orchestration
# ---------------------------------------------------------------------------
def select_image_for_visual(visual: Dict[str, Any], brief: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    query = _enforce_query_length(_inject_brief_anchors(visual.get("query", ""), brief))
    semantic_desc = visual.get("semantic_description", "")

    candidates = gather_image_candidates(query)

    if not candidates:
        retry_query = _build_retry_query(query, semantic_desc)
        logger.info(f"[select_image] primary empty, retrying with query='{retry_query}'")
        candidates = gather_image_candidates(retry_query)

    if not candidates:
        logger.warning(f"[select_image] no candidates for visual_id={visual.get('visual_id')}")
        return None

    result = pick_best_candidate(candidates, query, semantic_desc)
    chosen = result.get("candidate")

    if ENABLE_LLM_RERANK and result.get("score", 0) < LLM_RERANK_UNCERTAIN_THRESHOLD:
        ranked = result.get("ranked", [])
        top_n = [c for c, _ in ranked[:5]] if ranked else []
        if top_n:
            rerank_idx = llm_rerank_top_candidates(top_n, semantic_desc, brief)
            if rerank_idx is not None:
                chosen = top_n[rerank_idx]
                logger.info(f"[select_image] llm_rerank overrode to index={rerank_idx}")

    if not chosen:
        return None

    return {
        "visual_id": visual.get("visual_id"),
        "type": visual.get("type"),
        "placement": visual.get("placement"),
        "source_url": chosen.get("source_url"),
        "source_page": chosen.get("source_page"),
        "attribution": chosen.get("attribution"),
        "provider": chosen.get("provider"),
        "query_used": chosen.get("query_used"),
        "score": result.get("score"),
        "tier": result.get("tier"),
    }


def upload_image_to_s3(source_url: str, onepager_id: str, visual_id: str) -> Optional[str]:
    try:
        image_bytes = _http_get_bytes(source_url, tag="img_download")
        if not image_bytes:
            logger.warning(f"[s3_upload] failed to download {source_url}")
            return None

        if len(image_bytes) > MAX_BASE64_EMBED_BYTES:
            logger.info(f"[s3_upload] image too large ({len(image_bytes)} bytes), skipping embed")

        ext = "jpg"
        if source_url.lower().endswith(".png"):
            ext = "png"
        elif source_url.lower().endswith(".webp"):
            ext = "webp"

        key = f"{ASSETS_PREFIX}/{onepager_id}/{visual_id}.{ext}"
        s3_client.put_object(
            Bucket=ASSETS_BUCKET,
            Key=key,
            Body=image_bytes,
            ContentType=f"image/{ext}",
        )

        s3_url = f"https://{ASSETS_BUCKET}.s3.amazonaws.com/{key}"
        logger.info(f"[s3_upload] uploaded visual_id={visual_id} key={key}")
        return s3_url

    except Exception as e:
        logger.exception(f"[s3_upload] failed for visual_id={visual_id}: {e}")
        return None


def resolve_visuals(visual_plan: List[Dict[str, Any]], brief: Dict[str, Any], onepager_id: str) -> List[Dict[str, Any]]:
    resolved = []
    for visual in visual_plan:
        if visual.get("type") == "icon":
            resolved.append({
                "visual_id": visual.get("visual_id"),
                "type": "icon",
                "placement": visual.get("placement"),
                "icon_name": visual.get("icon_name"),
                "s3_url": None,
                "source_url": None,
            })
            continue

        selected = select_image_for_visual(visual, brief)
        if not selected:
            resolved.append({
                "visual_id": visual.get("visual_id"),
                "type": visual.get("type"),
                "placement": visual.get("placement"),
                "s3_url": None,
                "source_url": None,
                "error": "no_candidate_found",
            })
            continue

        s3_url = upload_image_to_s3(selected["source_url"], onepager_id, selected["visual_id"])
        resolved.append({
            **selected,
            "s3_url": s3_url,
        })

    return resolved


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        payload = parse_event(event)

        project_id = str(payload.get("project_id", "")).strip()
        session_id = str(payload.get("session_id", "")).strip()
        onepager_answers = payload.get("onepager_answers", {})

        if not project_id:
            return response(400, {"success": False, "error": "Missing required field: project_id"})
        if not session_id:
            return response(400, {"success": False, "error": "Missing required field: session_id"})
        if not onepager_answers:
            return response(400, {"success": False, "error": "Missing required field: onepager_answers"})

        user_id = get_user_id_from_session(session_id)
        if not user_id:
            return response(401, {"success": False, "error": "Invalid session_id"})

        facts = load_facts(project_id)
        brief = build_onepager_brief(facts, onepager_answers)

        missing = validate_brief(brief)
        if missing:
            return response(400, {
                "success": False,
                "error": f"Missing required brief fields: {', '.join(missing)}",
                "missing_fields": missing,
            })

        additional_payload = {
            "seo_payload": payload.get("seo_payload"),
            "keyword_bundle": payload.get("keyword_bundle"),
            "extra_instructions": payload.get("extra_instructions"),
        }

        logger.info(f"[handler] generating onepager for project_id={project_id}")
        onepager_json = generate_onepager_json(brief, additional_payload)

        onepager_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        visual_plan = onepager_json.get("visual_plan", [])
        resolved_visuals: List[Dict[str, Any]] = []
        if visual_plan:
            logger.info(f"[handler] resolving {len(visual_plan)} visuals")
            resolved_visuals = resolve_visuals(visual_plan, brief, onepager_id)

        item = sanitize_for_dynamodb({
            "project_id": project_id,
            ONEPAGER_SORT_KEY_NAME: onepager_id,
            "user_id": user_id,
            "title": onepager_json.get("title", ""),
            "slug": onepager_json.get("slug", ""),
            "status": "generated",
            "onepager_brief": brief,
            "onepager_output": onepager_json,
            "resolved_visuals": resolved_visuals,
            "generation_metadata": {
                "model_id": BEDROCK_TEXT_MODEL_ID,
                "region": BEDROCK_REGION,
                "used_company_info": brief.get("use_company_info", False),
            },
            "created_at": now,
            "updated_at": now,
        })

        onepager_table.put_item(Item=item)
        logger.info(f"[handler] saved onepager_id={onepager_id}")

        return response(200, {
            "success": True,
            "project_id": project_id,
            "onepager_id": onepager_id,
            "title": onepager_json.get("title", ""),
            "slug": onepager_json.get("slug", ""),
            "onepager_brief": brief,
            "onepager_output": onepager_json,
            "resolved_visuals": resolved_visuals,
            "generation_metadata": {
                "model_id": BEDROCK_TEXT_MODEL_ID,
                "region": BEDROCK_REGION,
                "used_company_info": brief.get("use_company_info", False),
            },
            "created_at": now,
        })

    except ValueError as ve:
        return response(400, {"success": False, "error": str(ve)})
    except json.JSONDecodeError:
        return response(400, {"success": False, "error": "Invalid JSON payload"})
    except Exception as exc:
        logger.exception(f"[handler] unhandled error: {exc}")
        return response(500, {
            "success": False,
            "error": "Failed to generate one-pager",
            "details": str(exc),
        })
