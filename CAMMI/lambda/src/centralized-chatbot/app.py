#Cursor code
import json
import boto3
import os
from boto3.dynamodb.conditions import Attr, Key
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, TypedDict
from decimal import Decimal
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================================
# INFRASTRUCTURE SETUP (UNCHANGED)
# ============================================================================

dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", "us-east-1")
bedrock_runtime = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)
stepfunctions = boto3.client("stepfunctions")
STATE_MACHINE_ARN = "arn:aws:states:us-east-1:687088702813:stateMachine:unified-state-machine"

FACTS_TABLE_NAME = os.environ.get('FACTS_TABLE_NAME', 'facts-table')
STATE_TABLE_NAME = os.environ.get('STATE_TABLE_NAME', 'project-state-table')
USERS_TABLE_NAME = os.environ.get('USERS_TABLE', 'users-table')
S3_BUCKET = os.environ.get('S3_BUCKET', 'cammi-devprod')
CONVERSATION_TABLE_NAME = os.environ.get('CONVERSATION_TABLE_NAME', 'conversation-history-table')
SECTION_STATE_TABLE_NAME = os.environ.get('SECTION_STATE_TABLE_NAME', 'clarify-align-state-table')
BEDROCK_MODEL_ID = 'us.anthropic.claude-sonnet-4-20250514-v1:0'
facts_table = dynamodb.Table(FACTS_TABLE_NAME)
state_table = dynamodb.Table(STATE_TABLE_NAME)
users_table = dynamodb.Table(USERS_TABLE_NAME)
conversation_table = dynamodb.Table(CONVERSATION_TABLE_NAME)
section_state_table = dynamodb.Table(SECTION_STATE_TABLE_NAME)

# Section definitions
CLARIFY_DOCUMENTS = ["GTM", "SMP", "MR", "BRAND", "MESSAGING", "ICP", "SR", "KMF", "BS", "ICP2"]
ALIGN_DOCUMENTS = ["CC", "QMP"]
SECTION_CLARIFY = "clarify"
SECTION_ALIGN = "align"
MIN_CLARIFY_DOCS_FOR_ALIGN = 2

ALIGN_INTRO_ONCE_TEXT = (
    "Welcome to **Align**! 🎉\n\n"
    "You've put in the work in Clarify — now this is where your strategy turns into a plan you can actually execute.\n\n"
    "The Align section has two documents: your **Content Calendar** and your **Quarterly Marketing Plan**. "
    "Both are built directly from everything you've already shared with me — your audience, your messaging, your goals — "
    "so you're not starting from scratch, you're building on what's already there.\n\n"
    "Your **Content Calendar** maps out what content you're putting out, when, and across which channels — "
    "organized by funnel stage so nothing falls through the cracks. "
    "Your **Quarterly Marketing Plan** takes your strategy and breaks it into a focused 90-day execution plan "
    "with clear priorities and actions."
)

# ============================================================================
# FACT UNIVERSE (UNCHANGED)
# ============================================================================

FACT_UNIVERSE = {
    "business.name": "Legal or common name of the business",
    "business.description_short": "Brief one-line description",
    "business.description_long": "Detailed business description",
    "business.industry": "Primary industry or sector",
    "business.stage": "Stage of business (startup, growth, mature)",
    "business.business_model": "How the business makes money",
    "business.pricing_position": "Pricing strategy positioning",
    "business.geography": "Primary business location or market",
    "business.start_date": "Business start date or launch date",
    "business.end_date_or_milestone": "Target end date or major milestone",
    "product.type": "Type of product or service",
    "product.core_offering": "Main product or service offering",
    "product.value_proposition_short": "Brief value proposition",
    "product.value_proposition_long": "Detailed value proposition",
    "product.problems_solved": "Problems the product solves",
    "product.unique_differentiation": "What makes the product unique",
    "product.strengths": "Product or company strengths",
    "product.weaknesses": "Product or company weaknesses",
    "customer.primary_customer": "Primary target customer description",
    "customer.buyer_roles": "Roles of people who buy",
    "customer.user_roles": "Roles of people who use the product",
    "customer.decision_maker": "Who makes the final purchase decision",
    "customer.buyer_goals": "What buyers are trying to achieve",
    "customer.buyer_pressures": "Pressures or constraints on buyers",
    "customer.industries": "Industries of target customers",
    "customer.company_size": "Size of target customer companies",
    "customer.geography": "Geographic location of customers",
    "customer.information_sources": "Where customers find information",
    "customer.problems": "Key problems customers face",
    "customer.pains": "Specific pains or frustrations",
    "customer.current_solutions": "How customers solve problems today",
    "customer.solution_gaps": "Gaps in current solutions",
    "market.competitors": "Direct competitors",
    "market.alternatives": "Alternative solutions",
    "market.why_alternatives_fail": "Why alternatives don't work well",
    "market.market_size_estimate": "Estimated market size",
    "market.trends_or_shifts": "Market trends or shifts",
    "market.opportunities": "Market opportunities",
    "market.threats": "Market threats or risks",
    "strategy.short_term_goals": "Goals for next 12 months",
    "strategy.long_term_vision": "3-5 year vision",
    "strategy.success_definition": "How success is defined",
    "strategy.priorities": "Top strategic priorities",
    "strategy.gtm_focus": "Go-to-market focus and strategy",
    "strategy.marketing_objectives": "Marketing objectives",
    "strategy.user_growth_priorities": "User growth priorities",
    "strategy.marketing_tools": "Marketing tools and channels",
    "strategy.marketing_budget": "Marketing budget",
    "brand.mission": "Company mission statement",
    "brand.vision": "Company vision statement",
    "brand.tone_personality": "Brand tone and personality",
    "brand.values_themes": "Brand values and themes",
    "brand.vibes_to_avoid": "Brand vibes to avoid",
    "brand.key_messages": "Key brand messages",
    "revenue.pricing_position": "Pricing position in market",
    "revenue.average_contract_value": "Average contract value",
    "revenue.market_size": "Total addressable market size",
    "revenue.marketing_budget": "Marketing budget allocation",
    "assets.approved_customers": "Approved customer names",
    "assets.case_studies": "Available case studies",
    "assets.videos": "Video assets",
    "assets.logos": "Customer or partner logos",
    "assets.quotes": "Customer quotes or testimonials",
    "assets.brag_points": "Notable achievements",
    "assets.visual_assets": "Visual assets available",
    "assets.spokesperson_name": "Company spokesperson name",
    "assets.spokesperson_role": "Spokesperson role or title",
    "content.calendar_timeframe": "Time period the content calendar covers",
    "content.special_activities": "Special events, launches, or activities to include in content calendar",
    "content.pillars": "Key content themes or pillars",
    "content.channels": "Content distribution channels (LinkedIn, blog, email, etc.)",
    "content.formats": "Content formats to be used (articles, infographics, videos, etc.)",
    "content.funnel_stages_priority": "Which funnel stages to prioritize (Top, Middle, Bottom)",
    "strategy.quarter_timeframe": "Specific quarter being planned (e.g., Q4 2024)",
    "strategy.quarterly_special_activities": "Major activities or launches planned for the quarter",
    "strategy.quarterly_goals": "Five specific measurable goals for the quarter",
    "strategy.kpi_targets": "Key performance indicator targets for quarterly goals",
    "strategy.marketing_team_structure": "Marketing team roles and owners for tactical execution"
}

    
# ============================================================================
# GLOBAL HARVESTERS (UNCHANGED)
# ============================================================================

GLOBAL_HARVESTERS = {
    "Q01_BUSINESS_OVERVIEW": {
        "question": "Let's start with the basics — what's your company called and what does it do? I'd love to hear both the quick elevator pitch and the bigger picture of what you're building.",
        "primary_facts": ["business.name", "business.description_short", "business.description_long", "business.industry"],
        "secondary_facts": ["business.business_model", "product.type"]
    },
    "Q02_CORE_OFFERING": {
        "question": "Tell me about what you actually sell or offer — and more importantly, why should someone care? Like, what's the thing that makes a customer go \"yes, I need this\"?",
        "primary_facts": ["product.core_offering", "product.value_proposition_short", "product.value_proposition_long"],
        "secondary_facts": ["product.type"]
    },
    "Q03_PROBLEM_SOLUTION": {
        "question": "Every great business solves a real problem. What's the one you're tackling, and when someone picks you over the other options out there — what's the reason? What makes you the one they go with?",
        "primary_facts": ["product.problems_solved", "product.unique_differentiation"],
        "secondary_facts": ["market.why_alternatives_fail"]
    },
    "Q04_BUSINESS_STAGE": {
        "question": "I'd love to know where you are in your journey — are you just getting off the ground, in full growth mode, or more established at this point? When did things kick off, and what's the next big milestone you're pushing toward?",
        "primary_facts": ["business.stage", "business.start_date", "business.end_date_or_milestone"],
        "secondary_facts": []
    },
    "Q05_PRICING": {
        "question": "Let's talk pricing — do you position yourself as the affordable option, somewhere in the middle, or more on the premium side? And roughly, what does a typical deal or contract look like in terms of value?",
        "primary_facts": ["business.pricing_position", "revenue.pricing_position", "revenue.average_contract_value"],
        "secondary_facts": []
    },
    "Q06_TARGET_CUSTOMER": {
        "question": "Now let's talk about who you're really going after. If you could describe your dream customer — what industry are they in, how big is their company, and where in the world are they?",
        "primary_facts": ["customer.primary_customer", "customer.industries", "customer.company_size", "customer.geography"],
        "secondary_facts": ["business.geography"]
    },
    "Q07_BUYER_ROLES": {
        "question": "When a deal actually happens, walk me through how it works — who's the one making the final call to buy, who else is involved in that decision, and who's the person that actually ends up using your product day-to-day?",
        "primary_facts": ["customer.buyer_roles", "customer.decision_maker", "customer.user_roles"],
        "secondary_facts": []
    },
    "Q08_BUYER_MOTIVATIONS": {
        "question": "Think about the people who buy from you — what are they really trying to accomplish? And on the flip side, what pressures or frustrations are they dealing with that push them to look for something like what you offer?",
        "primary_facts": ["customer.buyer_goals", "customer.buyer_pressures"],
        "secondary_facts": []
    },
    "Q09_CUSTOMER_PROBLEMS": {
        "question": "What's the single biggest headache your customers are dealing with before they find you? And when you look at the solutions they've tried before — what's missing? What are those solutions getting wrong?",
        "primary_facts": ["customer.problems", "customer.pains", "customer.solution_gaps"],
        "secondary_facts": []
    },
    "Q10_CURRENT_SOLUTIONS": {
        "question": "Before your customers find you, how are they dealing with this problem on their own? Are they using some other tool, doing it manually, or just living with it? And when they start looking for something better, where do they actually go — Google, LinkedIn, asking peers, something else?",
        "primary_facts": ["customer.current_solutions", "customer.information_sources"],
        "secondary_facts": []
    },
    "Q11_COMPETITORS": {
        "question": "Let's talk about the competitive landscape. Who are the names that come up most when your customers are comparing options? Both the direct competitors and the alternatives people consider instead of going with you.",
        "primary_facts": ["market.competitors", "market.alternatives"],
        "secondary_facts": []
    },
    "Q12_COMPETITIVE_ADVANTAGE": {
        "question": "Be honest with me here — what are you genuinely great at, and where do you know you could be stronger? And when customers go with one of those alternatives instead, what usually goes wrong for them — why do those options fall short?",
        "primary_facts": ["product.strengths", "product.weaknesses", "market.why_alternatives_fail"],
        "secondary_facts": []
    },
    "Q13_MARKET_LANDSCAPE": {
        "question": "Zooming out a bit — do you have a sense of how big your market is? And what's shifting in your space right now — any trends that excite you, new opportunities opening up, or things that keep you up at night?",
        "primary_facts": ["market.market_size_estimate", "market.trends_or_shifts", "market.opportunities", "market.threats"],
        "secondary_facts": ["revenue.market_size"]
    },
    "Q14_SHORT_TERM_GOALS": {
        "question": "Looking at the next 12 months — what are the big things you're trying to make happen? If you had to pick your top priorities and goals for the year ahead, what would they be?",
        "primary_facts": ["strategy.short_term_goals", "strategy.priorities"],
        "secondary_facts": ["strategy.marketing_objectives", "strategy.user_growth_priorities"]
    },
    "Q15_LONG_TERM_VISION": {
        "question": "Now let's think bigger — where do you want this business to be in 3 to 5 years? And when you picture \"we made it\" — what does that actually look like for you? What's your definition of success?",
        "primary_facts": ["strategy.long_term_vision", "strategy.success_definition"],
        "secondary_facts": []
    },
    "Q16_GTM_STRATEGY": {
        "question": "How are you actually getting customers through the door right now? Whether it's outbound, content, ads, referrals, partnerships — whatever it is. And are there specific tools or platforms you're relying on to make that happen?",
        "primary_facts": ["strategy.gtm_focus", "strategy.marketing_tools"],
        "secondary_facts": ["strategy.marketing_budget", "revenue.marketing_budget"]
    },
    "Q17_BRAND_MISSION": {
        "question": "This one's a bit deeper — why does your company exist beyond making money? What's the mission that drives you? And if you zoom out to the big picture, what's the vision for what you want this to become?",
        "primary_facts": ["brand.mission", "brand.vision"],
        "secondary_facts": []
    },
    "Q18_BRAND_PERSONALITY": {
        "question": "Imagine your brand is a person at a dinner party — how do they talk, what do they stand for, what's their energy like? Are you more playful or serious, bold or understated? And is there a vibe you definitely want to stay away from?",
        "primary_facts": ["brand.tone_personality", "brand.values_themes"],
        "secondary_facts": ["brand.vibes_to_avoid"]
    },
    "Q19_KEY_MESSAGES": {
        "question": "If someone walks away from your website or a conversation with your team and they can only remember one or two things — what do you want those things to be? What's the core message that should stick?",
        "primary_facts": ["brand.key_messages"],
        "secondary_facts": []
    },
    "Q20_ASSETS": {
        "question": "Let's take stock of what you've already got to work with. Do you have any customer stories, testimonials, case studies, logos you can show off, videos, or maybe a go-to spokesperson? Even rough stuff counts — I just want to know what's in the toolkit.",
        "primary_facts": ["assets.approved_customers", "assets.case_studies", "assets.quotes", "assets.brag_points"],
        "secondary_facts": ["assets.videos", "assets.logos", "assets.visual_assets", "assets.spokesperson_name", "assets.spokesperson_role"]
    },
    "Q21_CONTENT_CALENDAR_TIMEFRAME": {
        "question": "For your content calendar — what timeframe are we planning for? Like the next month, quarter, or longer? And is there anything specific we should build around — a product launch, an event, a seasonal push, anything like that?",
        "primary_facts": ["content.calendar_timeframe", "content.special_activities"],
        "secondary_facts": []
    },
    "Q22_CONTENT_STRATEGY": {
        "question": "Let's shape your content plan. What are the main topics or themes you want to be known for? Where are you going to publish — LinkedIn, your blog, email, somewhere else? And what's the main goal — getting new eyeballs on your brand, warming up people who already know you, or pushing them to actually buy?",
        "primary_facts": ["content.pillars", "content.channels", "content.formats", "content.funnel_stages_priority"],
        "secondary_facts": ["strategy.marketing_tools"]
    },
    "Q23_QUARTERLY_TIMEFRAME": {
        "question": "Which quarter are we planning for — like Q1 2025, Q2, etc.? And what's happening during that quarter — any big launches, campaigns, events, or milestones that the plan should be built around?",
        "primary_facts": ["strategy.quarter_timeframe", "strategy.quarterly_special_activities"],
        "secondary_facts": ["business.start_date"]
    },
    "Q24_QUARTERLY_GOALS_KPIS": {
        "question": "For this quarter, what are the 3 to 5 things you really want to accomplish? Be as specific as you can. And how will you actually know if it's working — what numbers or metrics are you going to keep an eye on?",
        "primary_facts": ["strategy.quarterly_goals", "strategy.kpi_targets"],
        "secondary_facts": ["strategy.short_term_goals", "strategy.success_definition"]
    },
    "Q25_MARKETING_TEAM": {
        "question": "Tell me about your marketing team — who's on it and what does each person own? Even if it's just you wearing all the hats, that's totally fine — I just want to know who's doing what so the plan is realistic.",
        "primary_facts": ["strategy.marketing_team_structure"],
        "secondary_facts": []
    }
}

# ============================================================================
# DOCUMENT REQUIREMENTS AND METADATA (UNCHANGED)
# ============================================================================

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
            "market.market_size_estimate",
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
    },
    "CC": {
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
    "QMP": {
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
    }
}

DOCUMENT_DESCRIPTIONS = {
    "GTM": {
        "name": "Go-To-Market",
        "short": "Launch and scale your product in the market",
        "description": "This is the most comprehensive and execution-oriented document, integrating strategy, branding, marketing, sales, and growth into one system."
    },
    "ICP": {
        "name": "Ideal Customer Profile",
        "short": "Define who to pursue and who to avoid",
        "description": "This document defines who the business should actively pursue and who it should avoid."
    },
    "ICP2": {
        "name": "Persona Deep Dive",
        "short": "Understand how your ideal customer thinks and decides",
        "description": "A deep, human-level view of a specific ideal customer persona with psychographic insights."
    },
    "MESSAGING": {
        "name": "Messaging Document",
        "short": "Define what you communicate and how you articulate value",
        "description": "Defines what the company communicates and how value is articulated."
    },
    "BRAND": {
        "name": "Brand Document",
        "short": "Establish your official brand identity system",
        "description": "Defines the official identity system of the brand including voice and tone."
    },
    "MR": {
        "name": "Market Research",
        "short": "Understand your market with data-backed insights",
        "description": "Provides a fact-based understanding of the market environment."
    },
    "KMF": {
        "name": "Key Messaging Framework",
        "short": "Organize messaging into a repeatable structure",
        "description": "Organizes messaging into a clear, repeatable structure."
    },
    "SR": {
        "name": "Strategy Roadmap",
        "short": "Connect vision to milestones and execution",
        "description": "Outlines the strategic direction and execution plan over time."
    },
    "SMP": {
        "name": "Strategic Marketing Plan",
        "short": "Plan how marketing drives business growth",
        "description": "Details how marketing will systematically drive business growth."
    },
    "BS": {
        "name": "Brand Strategy",
        "short": "Define why your brand exists and how it wins",
        "description": "Explains why the brand exists and how it wins in the market."
    },
    "CC": {
        "name": "Content Calendar",
        "short": "Plan and schedule content across channels and funnel stages",
        "description": "A structured calendar that organizes content publishing across channels, funnel stages, and time periods. It aligns content themes with marketing goals and ensures consistent, strategic content distribution."
    },
    "QMP": {
        "name": "Quarterly Marketing Plan",
        "short": "Set quarterly goals, KPIs, and tactical execution plans",
        "description": "A comprehensive quarterly execution plan that defines 5 specific marketing goals, assigns KPIs to measure success, and breaks down tactics with owners and timelines for systematic implementation."
    }
}

DOCUMENT_PROGRESSION = {
    "ICP": {"natural_next": ["ICP2", "MESSAGING", "GTM"], "reasoning": {"ICP2": "Dive deeper into your customer persona's psychology", "MESSAGING": "Craft messaging that speaks to your ideal customer", "GTM": "Build a go-to-market plan to reach your ideal customers"}},
    "ICP2": {"natural_next": ["MESSAGING", "KMF"], "reasoning": {"MESSAGING": "Use persona insights to craft compelling messaging", "KMF": "Structure messaging into a repeatable framework"}},
    "MESSAGING": {"natural_next": ["KMF", "BRAND"], "reasoning": {"KMF": "Organize messaging into a structured framework", "BRAND": "Define visual and verbal identity for your messaging"}},
    "BRAND": {"natural_next": ["BS", "KMF"], "reasoning": {"BS": "Build strategic positioning behind your brand", "KMF": "Structure brand messaging into a framework"}},
    "MR": {"natural_next": ["GTM", "ICP", "SR"], "reasoning": {"GTM": "Use market insights for go-to-market planning", "ICP": "Define your ideal customer from market research", "SR": "Create a roadmap informed by market trends"}},
    "KMF": {"natural_next": ["SMP", "GTM"], "reasoning": {"SMP": "Turn messaging framework into marketing execution", "GTM": "Build go-to-market strategy using structured messaging"}},
    "SR": {"natural_next": ["SMP", "GTM", "QMP"], "reasoning": {"SMP": "Create marketing plan aligned with your roadmap", "GTM": "Execute strategy with a go-to-market plan", "QMP": "Break down your strategy into quarterly execution plans"}},
    "SMP": {"natural_next": ["GTM", "QMP", "CC"], "reasoning": {"GTM": "Bring everything together in a comprehensive GTM document", "QMP": "Create quarterly execution plans from your strategic marketing plan", "CC": "Schedule and organize content to execute your marketing strategy"}},
    "GTM": {"natural_next": ["SR", "SMP", "QMP", "CC"], "reasoning": {"SR": "Plan strategic milestones for GTM execution", "SMP": "Detail marketing execution from GTM strategy", "QMP": "Break GTM into quarterly tactical plans", "CC": "Plan content calendar to support GTM execution"}},
    "BS": {"natural_next": ["BRAND", "SMP"], "reasoning": {"BRAND": "Create visual/verbal identity for brand strategy", "SMP": "Build marketing plan for brand strategy"}},
    "CC": {"natural_next": ["QMP", "SMP"], "reasoning": {"QMP": "Turn content plans into quarterly execution with KPIs and tactics", "SMP": "Align content calendar with broader strategic marketing initiatives"}},
    "QMP": {"natural_next": ["CC", "SR"], "reasoning": {"CC": "Schedule content to support quarterly goals and tactics", "SR": "Plan long-term roadmap after completing quarterly execution"}}
}

# ============================================================================
# CAMMI SCHEDULER / CAMPAIGN KNOWLEDGE
# ============================================================================

SCHEDULER_URL = "https://dev.d58o9xmomxg8r.amplifyapp.com/dashboard/scheduler"

SCHEDULER_KNOWLEDGE = """CAMMI PLATFORM CONTEXT:
This chatbot is the Document Generation module inside the CAMMI web app. It is one of several modules.
The sidebar of CAMMI has these sections:
- Document Generation: Clarify, Align, Mobilize, Monitor, Iterate (each opens this chatbot)
- Tools: Lead Calculator, Scheduler
- User Feedback: Feedback, Help

SCHEDULER MODULE (separate from this chatbot):
The Scheduler lets users plan, create, and optimize content campaigns with AI.
Features:
- Quick Post: Create and schedule a single LinkedIn post instantly.
- Create New Campaign: Import a URL or paste website text — CAMMI analyses the content and builds an SEO-optimized campaign. Users can also write their own idea or use existing brand documents.
- Use Existing Campaign: Continue working on an existing campaign and generate more content.
- View Calendar: View and manage all scheduled posts in a calendar timeline.

Campaign types (based on goal): Awareness, Consideration, Conversion.
Users must connect their LinkedIn profile in the Scheduler before using it.

IMPORTANT: If the user mentions campaigns, scheduling posts, LinkedIn posting, content calendar,
running a campaign, social media campaigns, posting schedule, or anything related to executing/distributing
content on LinkedIn — guide them to the Scheduler. You CANNOT handle campaigns yourself.
Direct link: """ + SCHEDULER_URL + """
"""

# ============================================================================
# LAYER DEFINITIONS
# ============================================================================

LAYER_DISCOVERY = "DISCOVERY"
LAYER_QUESTIONING = "QUESTIONING"
LAYER_REVIEW = "REVIEW"
LAYER_GENERATION = "GENERATION"
LAYER_POST_GENERATION = "POST_GENERATION"

# Exact frontend trigger for the Improve Now button
IMPROVE_QUALITY_TRIGGER = "The user wants to improve the quality of the selected document"

# ============================================================================
# STATE SCHEMA
# ============================================================================

class ConversationState(TypedDict):
    project_id: str
    user_id: str
    session_id: str
    user_message: str
    conversation_history: List[Dict[str, str]]
    facts: Dict[str, Dict[str, Any]]
    active_layer: str
    layer_context: Dict[str, Any]
    active_document: Optional[str]
    generating_document: Optional[str]
    interrupted_document: Optional[str]
    completed_documents: List[str]
    current_tab: str
    current_question_id: Optional[str]
    asked_questions: List[str]
    pending_questions: List[str]
    question_attempts: Dict[str, int]
    skipped_questions: List[str]
    last_agent: str
    response: str
    should_end: bool


# ============================================================================
# DYNAMODB FLOAT/DECIMAL SANITIZER (UNCHANGED)
# ============================================================================

def sanitize_for_dynamodb(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: sanitize_for_dynamodb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_for_dynamodb(i) for i in obj]
    elif isinstance(obj, Decimal):
        return obj
    return obj


def sanitize_from_dynamodb(obj):
    if isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    elif isinstance(obj, dict):
        return {k: sanitize_from_dynamodb(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [sanitize_from_dynamodb(i) for i in obj]
    return obj


# ============================================================================
# DATABASE OPERATIONS (UNCHANGED)
# ============================================================================

def get_user_id_from_session(session_id: str) -> Optional[str]:
    try:
        response = users_table.scan(FilterExpression=Attr('session_id').eq(session_id))
        items = response.get('Items', [])
        if items:
            return items[0].get('id')
        return None
    except Exception as e:
        print(f"Error validating session: {e}")
        return None


def load_project_state(project_id: str) -> Dict[str, Any]:
    try:
        response = state_table.get_item(Key={'project_id': project_id})
        item = response.get('Item', {})
        return sanitize_from_dynamodb(item)
    except Exception as e:
        print(f"Error loading project state: {e}")
        return {}


def initialize_project_state(project_id: str, user_id: str, session_id: str) -> Dict[str, Any]:
    now = datetime.utcnow().isoformat()
    initial_state = {
        'project_id': project_id,
        'user_id': user_id,
        'session_id': session_id,
        'active_layer': LAYER_DISCOVERY,
        'layer_context': {},
        'active_document': None,
        'generating_document': None,
        'interrupted_document': None,
        'completed_documents': [],
        'current_question_id': None,
        'asked_questions': [],
        'pending_questions': [],
        'question_attempts': {},
        'skipped_questions': [],
        'created_at': now,
        'updated_at': now
    }
    try:
        state_table.put_item(Item=sanitize_for_dynamodb(initial_state))
        return initial_state
    except Exception as e:
        print(f"Error initializing project state: {e}")
        return initial_state


def save_project_state(state: ConversationState) -> bool:
    try:
        now = datetime.utcnow().isoformat()
        update_expression = """
            SET active_layer = :active_layer,
                layer_context = :layer_context,
                active_document = :active_document,
                generating_document = :generating_document,
                interrupted_document = :interrupted_document,
                completed_documents = :completed_documents,
                current_question_id = :current_question_id,
                asked_questions = :asked_questions,
                pending_questions = :pending_questions,
                question_attempts = :question_attempts,
                skipped_questions = :skipped_questions,
                updated_at = :updated_at
        """
        expression_values = sanitize_for_dynamodb({
            ':active_layer': state.get('active_layer', LAYER_DISCOVERY),
            ':layer_context': state.get('layer_context', {}),
            ':active_document': state.get('active_document'),
            ':generating_document': state.get('generating_document'),
            ':interrupted_document': state.get('interrupted_document'),
            ':completed_documents': state.get('completed_documents', []),
            ':current_question_id': state.get('current_question_id'),
            ':asked_questions': state.get('asked_questions', []),
            ':pending_questions': state.get('pending_questions', []),
            ':question_attempts': state.get('question_attempts', {}),
            ':skipped_questions': state.get('skipped_questions', []),
            ':updated_at': now
        })
        state_table.update_item(
            Key={'project_id': state['project_id']},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        return True
    except Exception as e:
        print(f"Error saving project state: {e}")
        return False


def load_facts(project_id: str) -> Dict[str, Dict[str, Any]]:
    facts = {}
    try:
        response = facts_table.query(KeyConditionExpression=Key('project_id').eq(project_id))
        for item in response.get('Items', []):
            fact_id = item.get('fact_id')
            if fact_id:
                facts[fact_id] = {
                    'value': item.get('value'),
                    'source': item.get('source', 'chat'),
                    'updated_at': item.get('updated_at')
                }
        while 'LastEvaluatedKey' in response:
            response = facts_table.query(
                KeyConditionExpression=Key('project_id').eq(project_id),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response.get('Items', []):
                fact_id = item.get('fact_id')
                if fact_id:
                    facts[fact_id] = {
                        'value': item.get('value'),
                        'source': item.get('source', 'chat'),
                        'updated_at': item.get('updated_at')
                    }
        return facts
    except Exception as e:
        print(f"Error loading facts: {e}")
        return {}


def save_fact(project_id: str, fact_id: str, value: str, source: str = 'chat') -> bool:
    if fact_id not in FACT_UNIVERSE:
        print(f"Invalid fact_id: {fact_id}")
        return False
    try:
        now = datetime.utcnow().isoformat()
        facts_table.put_item(Item={
            'project_id': project_id,
            'fact_id': fact_id,
            'value': value,
            'source': source,
            'updated_at': now
        })
        return True
    except Exception as e:
        print(f"Error saving fact: {e}")
        return False


def save_multiple_facts(project_id: str, facts_to_save: Dict[str, str], source: str = 'chat') -> int:
    now = datetime.utcnow().isoformat()
    items = []
    for fact_id, value in facts_to_save.items():
        if value and str(value).strip() and fact_id in FACT_UNIVERSE:
            items.append({
                'PutRequest': {
                    'Item': sanitize_for_dynamodb({
                        'project_id': project_id,
                        'fact_id': fact_id,
                        'value': str(value).strip(),
                        'source': source,
                        'updated_at': now
                    })
                }
            })
    if not items:
        return 0
    try:
        # batch_write_item supports up to 25 items per call
        for i in range(0, len(items), 25):
            batch = items[i:i+25]
            dynamodb.meta.client.batch_write_item(
                RequestItems={FACTS_TABLE_NAME: batch}
            )
        return len(items)
    except Exception as e:
        print(f"Error batch saving facts: {e}")
        # Fallback to individual saves
        saved_count = 0
        for fact_id, value in facts_to_save.items():
            if value and str(value).strip():
                if save_fact(project_id, fact_id, str(value).strip(), source):
                    saved_count += 1
        return saved_count


# ============================================================================
# CONVERSATION HISTORY (DynamoDB)
# ============================================================================

def load_conversation_history(project_id: str) -> List[Dict[str, str]]:
    try:
        response = conversation_table.query(
            KeyConditionExpression=Key('project_id').eq(project_id),
            ScanIndexForward=True  # ascending by timestamp
        )
        history = []
        for item in response.get('Items', []):
            history.append({
                'timestamp': item.get('timestamp', ''),
                'role': item.get('role', 'user').lower(),
                'content': item.get('content', '')
            })
        # Handle pagination for very long conversations
        while 'LastEvaluatedKey' in response:
            response = conversation_table.query(
                KeyConditionExpression=Key('project_id').eq(project_id),
                ScanIndexForward=True,
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response.get('Items', []):
                history.append({
                    'timestamp': item.get('timestamp', ''),
                    'role': item.get('role', 'user').lower(),
                    'content': item.get('content', '')
                })
        return history
    except Exception as e:
        print(f"Error loading conversation history: {e}")
        return []


def save_conversation_turn(project_id: str, role: str, message: str) -> bool:
    try:
        timestamp = datetime.utcnow().isoformat()
        conversation_table.put_item(Item={
            'project_id': project_id,
            'timestamp': timestamp,
            'role': role.lower(),
            'content': message
        })
        return True
    except Exception as e:
        print(f"Error saving conversation turn: {e}")
        return False


def format_conversation_for_llm(history: List[Dict[str, str]], limit: int = 10) -> str:
    recent = history[-limit:] if len(history) > limit else history
    formatted = []
    for turn in recent:
        role = turn.get('role', 'user').upper()
        content = turn.get('content', '')
        formatted.append(f"{role}: {content}")
    return "\n".join(formatted)


# ============================================================================
# LLM INVOCATION (UNCHANGED)
# ============================================================================

def invoke_bedrock(system_prompt: str, user_message: str, temperature: float = 0.3, max_tokens: int = 2048) -> str:
    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_message}],
            "temperature": temperature,
            "max_tokens": max_tokens
        })
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID, body=body,
            contentType="application/json", accept="application/json"
        )
        response_body = json.loads(response['body'].read())
        return response_body['content'][0]['text']
    except Exception as e:
        print(f"BEDROCK INVOKE ERROR [{type(e).__name__}] Model: {BEDROCK_MODEL_ID} | Error: {e}")
        return ""


def invoke_bedrock_json(system_prompt: str, user_message: str, temperature: float = 0.1, max_tokens: int = 4096) -> Dict[str, Any]:
    try:
        full_system = system_prompt + "\n\nIMPORTANT: Respond ONLY with valid JSON. No markdown, no explanations, no code blocks."
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "system": full_system,
            "messages": [{"role": "user", "content": user_message}],
            "temperature": temperature,
            "max_tokens": max_tokens
        })
        response = bedrock_runtime.invoke_model(
            modelId=BEDROCK_MODEL_ID, body=body,
            contentType="application/json", accept="application/json"
        )
        response_body = json.loads(response['body'].read())
        text = response_body['content'][0]['text']
        text = text.strip()
        while text.startswith('```'):
            if text.startswith('```json'):
                text = text[7:]
            elif text.startswith('```'):
                text = text[3:]
            text = text.strip()
        while text.endswith('```'):
            text = text[:-3]
            text = text.strip()
        text = text.strip()
        parsed = json.loads(text)
        return parsed
    except json.JSONDecodeError as e:
        print(f"BEDROCK JSON PARSE ERROR | Model: {BEDROCK_MODEL_ID} | Error: {e} | Raw response preview: {text[:200] if 'text' in locals() else 'N/A'}")
        return {}
    except Exception as e:
        print(f"BEDROCK INVOKE ERROR [{type(e).__name__}] Model: {BEDROCK_MODEL_ID} | Error: {e}")
        return {}


# ============================================================================
# SECTION STATE MANAGEMENT (Clarify/Align)
# ============================================================================

def get_current_section_state(project_id: str) -> Dict[str, Any]:
    """
    Retrieves the current section state for a project.
    Returns a dict with 'current_tab' and 'completed_documents' list.
    """
    try:
        response = section_state_table.get_item(Key={'project_id': project_id})
        if 'Item' in response:
            item = response['Item']
            print(f"🔍 SECTION STATE RAW DB ITEM: {dict(item)}")
            result = {
                'current_tab': item.get('current_tab', SECTION_CLARIFY),
                'completed_documents': item.get('completed_documents', []),
                'align_intro_shown': item.get('align_intro_shown', False)
            }
            print(f"🔍 SECTION STATE PARSED: current_tab={repr(result['current_tab'])}, align_intro_shown={repr(result['align_intro_shown'])}, type={type(result['align_intro_shown'])}")
            return result
        else:
            # Default: start in Clarify section with no completed docs
            return {
                'current_tab': SECTION_CLARIFY,
                'completed_documents': [],
                'align_intro_shown': False
            }
    except Exception as e:
        print(f"Error loading section state: {e}")
        return {
            'current_tab': SECTION_CLARIFY,
            'completed_documents': [],
            'align_intro_shown': False
        }


def update_section_state(project_id: str, current_tab: str, completed_documents: List[str], align_intro_shown: bool = False) -> bool:
    """
    Updates the section state for a project.
    """
    try:
        section_state_table.put_item(Item={
            'project_id': project_id,
            'current_tab': current_tab,
            'completed_documents': completed_documents,
            'align_intro_shown': align_intro_shown,
            'updated_at': datetime.utcnow().isoformat()
        })
        return True
    except Exception as e:
        print(f"Error updating section state: {e}")
        return False


def check_align_eligibility(completed_documents: List[str]) -> Dict[str, Any]:
    """
    Checks if the user is eligible to access Align section.
    Returns dict with 'is_eligible', 'clarify_count', 'min_required'.
    """
    clarify_completed = [doc for doc in completed_documents if doc in CLARIFY_DOCUMENTS]
    clarify_count = len(clarify_completed)
    is_eligible = clarify_count >= MIN_CLARIFY_DOCS_FOR_ALIGN

    return {
        'is_eligible': is_eligible,
        'clarify_count': clarify_count,
        'min_required': MIN_CLARIFY_DOCS_FOR_ALIGN,
        'clarify_completed': clarify_completed
    }


# ============================================================================
# DOCUMENT AND FACT UTILITY FUNCTIONS (UNCHANGED)
# ============================================================================

def get_document_display_name(doc_code: str) -> str:
    if doc_code and doc_code in DOCUMENT_REQUIREMENTS:
        return DOCUMENT_REQUIREMENTS[doc_code]['name']
    return doc_code or "Unknown"


def get_required_facts_for_document(doc_code: str) -> List[str]:
    if doc_code in DOCUMENT_REQUIREMENTS:
        return DOCUMENT_REQUIREMENTS[doc_code].get('required_facts', [])
    return []


def get_supporting_facts_for_document(doc_code: str) -> List[str]:
    if doc_code in DOCUMENT_REQUIREMENTS:
        return DOCUMENT_REQUIREMENTS[doc_code].get('supporting_facts', [])
    return []


def get_all_facts_for_document(doc_code: str) -> List[str]:
    required = get_required_facts_for_document(doc_code)
    supporting = get_supporting_facts_for_document(doc_code)
    return list(set(required + supporting))


def calculate_document_readiness(doc_code: str, facts: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    required_facts = get_required_facts_for_document(doc_code)
    supporting_facts = get_supporting_facts_for_document(doc_code)
    filled_required = [f for f in required_facts if f in facts and facts[f].get('value')]
    filled_supporting = [f for f in supporting_facts if f in facts and facts[f].get('value')]
    missing_required = [f for f in required_facts if f not in facts or not facts[f].get('value')]
    missing_supporting = [f for f in supporting_facts if f not in facts or not facts[f].get('value')]
    required_percentage = (len(filled_required) / len(required_facts) * 100) if required_facts else 100
    total_facts = len(required_facts) + len(supporting_facts)
    total_filled = len(filled_required) + len(filled_supporting)
    overall_percentage = (total_filled / total_facts * 100) if total_facts else 100
    return {
        'doc_code': doc_code,
        'doc_name': get_document_display_name(doc_code),
        'required_percentage': round(required_percentage, 1),
        'overall_percentage': round(overall_percentage, 1),
        'is_ready': len(missing_required) == 0,
        'filled_required': filled_required,
        'missing_required': missing_required,
        'filled_supporting': filled_supporting,
        'missing_supporting': missing_supporting,
        'total_required': len(required_facts),
        'total_supporting': len(supporting_facts)
    }


def get_harvesters_for_missing_facts(missing_facts: List[str]) -> List[str]:
    relevant_harvesters = []
    for harvester_id, harvester in GLOBAL_HARVESTERS.items():
        primary_facts = harvester.get('primary_facts', [])
        secondary_facts = harvester.get('secondary_facts', [])
        all_harvester_facts = primary_facts + secondary_facts
        overlap = set(all_harvester_facts) & set(missing_facts)
        if overlap:
            primary_overlap = set(primary_facts) & set(missing_facts)
            relevant_harvesters.append((harvester_id, len(overlap), len(primary_overlap)))
    relevant_harvesters.sort(key=lambda x: (x[2], x[1]), reverse=True)
    return [h[0] for h in relevant_harvesters]


def determine_pending_questions(
    doc_code: str,
    facts: Dict[str, Dict[str, Any]],
    asked_questions: List[str],
    skipped_questions: List[str] = None
) -> List[str]:
    if skipped_questions is None:
        skipped_questions = []
    readiness = calculate_document_readiness(doc_code, facts)
    missing_facts = readiness['missing_required'] + readiness['missing_supporting']
    if not missing_facts:
        return []
    relevant_harvesters = get_harvesters_for_missing_facts(missing_facts)
    pending = []
    reask = []
    for harvester_id in relevant_harvesters:
        if harvester_id in skipped_questions:
            continue
        if harvester_id not in asked_questions:
            pending.append(harvester_id)
        else:
            harvester = GLOBAL_HARVESTERS.get(harvester_id, {})
            harvester_primary = harvester.get('primary_facts', [])
            can_fill = any(f in missing_facts for f in harvester_primary)
            if can_fill:
                reask.append(harvester_id)
    return pending + reask


def build_business_profile_summary(facts: Dict[str, Dict[str, Any]]) -> str:
    profile_keys = [
        'business.name', 'business.description_short', 'business.description_long',
        'business.industry', 'business.stage', 'product.core_offering',
        'customer.primary_customer', 'product.problems_solved'
    ]
    parts = []
    for key in profile_keys:
        if key in facts and facts[key].get('value'):
            parts.append(f"{key.split('.')[-1]}: {facts[key]['value']}")
    return '; '.join(parts) if parts else 'No business information collected yet'


def format_facts_for_display(
    facts: Dict[str, Dict[str, Any]],
    doc_code: Optional[str] = None
) -> Tuple[str, Dict[str, str]]:
    if not facts:
        return "No information collected yet.", {}
    categories = {}
    relevant_facts = facts
    if doc_code:
        relevant_fact_ids = get_all_facts_for_document(doc_code)
        relevant_facts = {k: v for k, v in facts.items() if k in relevant_fact_ids}
    for fact_id, fact_data in relevant_facts.items():
        if not fact_data.get('value'):
            continue
        category = fact_id.split('.')[0].title()
        if category not in categories:
            categories[category] = []
        fact_name = FACT_UNIVERSE.get(fact_id, fact_id)
        categories[category].append({'id': fact_id, 'name': fact_name, 'value': fact_data['value']})
    if not categories:
        return "No information collected yet.", {}
    lines = []
    fact_number = 1
    fact_mapping = {}
    for category, category_facts in sorted(categories.items()):
        lines.append(f"\n**{category}**")
        for fact in category_facts:
            lines.append(f"  {fact_number}. {fact['name']}: {fact['value']}")
            fact_mapping[str(fact_number)] = fact['id']
            fact_number += 1
    return "\n".join(lines), fact_mapping


def build_document_list_text(state: ConversationState = None) -> str:
    completed = state.get('completed_documents', []) if state else []
    facts = state.get('facts', {}) if state else {}
    lines = []
    for code, desc in DOCUMENT_DESCRIPTIONS.items():
        if code in completed:
            continue
        line = f"**{desc['name']}** ({code}): {desc['short']}"
        if facts:
            readiness = calculate_document_readiness(code, facts)
            if readiness['is_ready']:
                line += " 🟢"
            elif readiness['required_percentage'] >= 50:
                line += f" 🟡 {readiness['required_percentage']:.0f}%"
            elif readiness['required_percentage'] > 0:
                line += f" ⚪ {readiness['required_percentage']:.0f}%"
        lines.append(line)
    if not lines:
        return "🎉 You've completed all available documents!"
    return "\n".join(lines)


# ============================================================================
# OPPORTUNISTIC FACT EXTRACTION (UNCHANGED)
# ============================================================================

def extract_facts_opportunistically(state: ConversationState, user_message: str) -> int:
    existing_facts = {k for k, v in state['facts'].items() if v.get('value')}
    priority_facts = [
        'business.name', 'business.description_short', 'business.description_long',
        'business.industry', 'business.stage', 'business.business_model',
        'product.core_offering', 'product.type', 'product.problems_solved',
        'product.value_proposition_short', 'product.unique_differentiation',
        'customer.primary_customer', 'customer.industries', 'customer.company_size',
        'customer.geography'
    ]
    missing_facts = {k: FACT_UNIVERSE[k] for k in priority_facts if k not in existing_facts}
    if not missing_facts:
        return 0
    fact_list_str = "\n".join([f"- {fid}: {desc}" for fid, desc in missing_facts.items()])
    system_prompt = f"""You are extracting business facts from a casual user message about THEIR business.

CRITICAL RULES — DO NOT extract from:
1. QUESTIONS: "what is X?", "tell me about X", "how does X work?"
2. HYPOTHETICALS: "if I were...", "what if...", "let's say..."
3. EXAMPLES: "like OnlyFans", "similar to Uber", "such as..."
4. NEGATIONS: "I'm not...", "we don't...", "not looking to..."
5. GENERAL TOPICS: Questions about concepts, competitors, or other companies

ONLY extract when the user is CLAIMING or STATING facts about THEIR OWN business.

Look for OWNERSHIP/IDENTITY markers:
- "I run...", "we are...", "my company is...", "our business..."
- "I sell...", "we help...", "I'm building...", "we serve..."
- First-person statements about their business

EXAMPLES:
✅ EXTRACT: "I run a tech startup called Acme" → business.name: "Acme"
✅ EXTRACT: "we help small businesses automate" → customer.company_size: "small businesses"
❌ DO NOT EXTRACT: "what is OnlyFans?" → nothing (this is a question)
❌ DO NOT EXTRACT: "tell me about SaaS" → nothing (asking to learn)
❌ DO NOT EXTRACT: "like Uber but for food" → nothing (example/comparison)

AVAILABLE FACTS:
{fact_list_str}

Respond with JSON:
{{"extracted_facts": {{"fact.id": "value or null"}}, "confidence": {{"fact.id": 0.0 to 1.0}}}}"""
    result = invoke_bedrock_json(system_prompt, f'User said: "{user_message}"')
    if result:
        extracted = result.get('extracted_facts', {})
        confidence = result.get('confidence', {})
        facts_to_save = {}
        for k, v in extracted.items():
            if v and str(v).strip() and k in FACT_UNIVERSE:
                # Higher confidence threshold for opportunistic extraction (0.75 vs 0.6)
                fact_conf = confidence.get(k, 0.8)
                if fact_conf >= 0.75:
                    facts_to_save[k] = str(v).strip()
        if facts_to_save:
            saved = save_multiple_facts(state['project_id'], facts_to_save, 'chat')
            for fact_id, value in facts_to_save.items():
                state['facts'][fact_id] = {
                    'value': value,
                    'source': 'chat',
                    'updated_at': datetime.utcnow().isoformat()
                }
            return saved
    return 0


# ============================================================================
# STATE INITIALIZATION (UNCHANGED)
# ============================================================================

def build_initial_state(
    project_id: str, user_id: str, session_id: str, user_message: str
) -> ConversationState:
    # Parallelize all four independent DB reads
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_state = executor.submit(load_project_state, project_id)
        future_facts = executor.submit(load_facts, project_id)
        future_history = executor.submit(load_conversation_history, project_id)
        future_section = executor.submit(get_current_section_state, project_id)

        db_state = future_state.result()
        facts = future_facts.result()
        conversation_history = future_history.result()
        section_state = future_section.result()

    if not db_state:
        db_state = initialize_project_state(project_id, user_id, session_id)
    state: ConversationState = {
        'project_id': project_id,
        'user_id': user_id,
        'session_id': session_id,
        'user_message': user_message,
        'conversation_history': conversation_history,
        'facts': facts,
        'active_layer': db_state.get('active_layer', LAYER_DISCOVERY),
        'layer_context': db_state.get('layer_context', {}),
        'active_document': db_state.get('active_document'),
        'generating_document': db_state.get('generating_document'),
        'interrupted_document': db_state.get('interrupted_document'),
        'completed_documents': db_state.get('completed_documents', []),
        'current_tab': section_state.get('current_tab', SECTION_CLARIFY),
        'align_intro_shown': section_state.get('align_intro_shown', False),
        'current_question_id': db_state.get('current_question_id'),
        'asked_questions': db_state.get('asked_questions', []),
        'pending_questions': db_state.get('pending_questions', []),
        'question_attempts': db_state.get('question_attempts', {}),
        'skipped_questions': db_state.get('skipped_questions', []),
        'last_agent': 'none',
        'response': '',
        'should_end': False
    }
    return state


def recover_state(state: ConversationState) -> ConversationState:
    if not state.get('active_layer'):
        state['active_layer'] = LAYER_DISCOVERY
    if state.get('active_layer') == LAYER_QUESTIONING and not state.get('active_document'):
        state['active_layer'] = LAYER_DISCOVERY
    if state.get('active_layer') == LAYER_GENERATION and not state.get('active_document'):
        state['active_layer'] = LAYER_DISCOVERY
    if not state.get('question_attempts'):
        state['question_attempts'] = {}
    if not state.get('layer_context'):
        state['layer_context'] = {}
    if not state.get('skipped_questions'):
        state['skipped_questions'] = []
    return state


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _get_last_system_message(state: ConversationState) -> str:
    for turn in reversed(state['conversation_history']):
        if turn.get('role') == 'assistant':
            return turn.get('content', '')
    return ''


def _build_state_summary(state: ConversationState) -> str:
    """Build state summary with most important signals FIRST."""
    facts = state['facts']
    facts_count = len([f for f in facts.values() if f.get('value')])
    completed = state.get('completed_documents', [])
    doc_code = state.get('active_document')
    current_q = state.get('current_question_id')
    lc = state.get('layer_context', {})

    summary_parts = []

    # Active document info (very important for routing)
    if doc_code:
        readiness = calculate_document_readiness(doc_code, facts)

        if readiness['is_ready']:
            readiness_status = "READY to generate"
        else:
            missing_count = len(readiness.get("missing_required", []))
            readiness_status = f"{missing_count} required facts missing"

        summary_parts.append(
            f"Active document: {get_document_display_name(doc_code)} ({doc_code}) — "
            f"{readiness['required_percentage']:.0f}% ready, {readiness_status}"
        )
    else:
        summary_parts.append("Active document: NONE selected")

    # Current question (critical for answer detection)
    if current_q and current_q in GLOBAL_HARVESTERS:
        summary_parts.append(
            f"Current harvester question being asked: "
            f"\"{GLOBAL_HARVESTERS[current_q]['question']}\""
        )
    else:
        summary_parts.append("Current harvester question: NONE")

    # Pending edit (critical for edit confirmation)
    if lc.get('pending_edit_fact'):
        fact_name = FACT_UNIVERSE.get(
            lc['pending_edit_fact'], lc['pending_edit_fact']
        )
        summary_parts.append(
            f"PENDING EDIT awaiting confirmation: {fact_name} → "
            f"\"{lc.get('pending_edit_value', '?')}\""
        )

    # Editing context
    if lc.get('editing_fact'):
        fact_name = FACT_UNIVERSE.get(
            lc['editing_fact'], lc['editing_fact']
        )
        summary_parts.append(
            f"Currently editing fact: {fact_name} (waiting for new value)"
        )

    # Explained document context
    if lc.get('explained_doc'):
        summary_parts.append(
            f"Last explained document: {lc['explained_doc']}"
        )

    # General stats
    summary_parts.append(f"Facts collected: {facts_count}")
    summary_parts.append(
        f"Completed documents: {', '.join(completed) if completed else 'none'}"
    )

    # Questions progress
    asked = len(state.get('asked_questions', []))
    pending = len(state.get('pending_questions', []))
    if doc_code and (asked > 0 or pending > 0):
        summary_parts.append(
            f"Questions: {asked} asked, {pending} remaining"
        )

    return "\n".join(summary_parts)


def _is_affirmative_message(message: str) -> bool:
    msg = (message or "").strip().lower().rstrip('.!,?')
    affirmative = {
        'yes', 'y', 'yeah', 'yep', 'sure', 'ok', 'okay', 'go ahead',
        'proceed', 'generate', 'create it', 'do it', 'sounds good',
        'looks good', 'approved', 'confirm'
    }
    return msg in affirmative or any(
        phrase in msg for phrase in ['go ahead', 'sounds good', 'looks good', 'create it', 'generate it']
    )


def _is_negative_message(message: str) -> bool:
    msg = (message or "").strip().lower().rstrip('.!,?')
    negative = {
        'no', 'n', 'nope', 'nah', 'not now', 'cancel', 'skip',
        'i will type', "i'll type", 'type myself', 'let me edit'
    }
    return msg in negative or any(
        phrase in msg for phrase in ['type myself', 'i will type', "i'll type", 'let me edit', 'not now']
    )


def _truncate_preview_value(value: str, max_len: int = 1400) -> str:
    text = str(value or '').strip().replace('\n', ' ')
    text = re.sub(r'\s+', ' ', text)
    # Improve preview must preserve full values; do not truncate with ellipsis.
    return text



# ============================================================================
# CHANGE 1 + 2: UNIFIED ROUTER (replaces master_router + all layer classifiers)
# ============================================================================

UNIFIED_ROUTER_PROMPT = """You are the routing brain of a business document assistant. You read context and decide ONE action.

══════════════════════════════════════════════════
CONTEXT
══════════════════════════════════════════════════

LAST SYSTEM ACTION: {last_action}
LAST SYSTEM MESSAGE: {last_system_message}

{state_summary}

RECENT CONVERSATION:
{conversation_history}

══════════════════════════════════════════════════
YOUR JOB
══════════════════════════════════════════════════

Read the user's message. Read what the system just said (LAST SYSTEM MESSAGE). Decide which action to take. The user's message is almost always a DIRECT RESPONSE to whatever the system just said — so always start by understanding what the system asked or offered.

══════════════════════════════════════════════════
ACTIONS (pick exactly one)
══════════════════════════════════════════════════

NAVIGATION:
  greet              → User is saying hello
  list_documents     → Show all available documents
  show_progress      → Show overall progress summary
  recommend_documents → User wants suggestions
  done               → User is finished / goodbye

BUSINESS INFO:
  describe_business  → User is voluntarily sharing business info (NOT answering a harvester question)

DOCUMENTS:
  select_document    → User wants to START/CREATE a specific document (set "document")
  inquire_document   → User wants to LEARN ABOUT a document, not commit (set "document")
  switch_document    → User wants to CHANGE to a different document (set "document")
  reject_recommendation → User is declining a document suggestion

QUESTIONING:
  start_questioning  → Begin/resume asking questions for active document
  process_answer     → User is answering the current question with business info
  help_question      → User needs help: confused, doesn't know, wants suggestions, wants explanation
  skip_question      → User wants to skip current question

REVIEW:
  show_facts         → User wants to see collected info
  edit_fact          → User wants to change a fact (set "edit_target" and optionally "edit_value")
  confirm_edit       → User confirms a pending edit
  cancel_edit        → User cancels a pending edit
  correct_edit       → User wants to correct the edit they just made (e.g., "wait no", "actually change it to")

GENERATION:
  generate_document  → User wants to generate NOW
  cancel_generation  → User was about to generate and changed mind
  decline_generation → User said no to generating but wants to stay with the document

PLATFORM:
  redirect_to_scheduler → User's intent is about EXECUTING or DISTRIBUTING content — not creating documents. This includes:
    • Campaigns (creating, running, managing, optimising, tracking, launching, pausing, editing campaigns)
    • Scheduling or publishing posts (on LinkedIn or social media in general)
    • Content calendar management (viewing, planning, organising a posting timeline)
    • LinkedIn-specific actions (connecting LinkedIn, posting on LinkedIn, LinkedIn strategy execution)
    • Social media distribution, content promotion, audience engagement via posts
    • Any mention of the Scheduler tool, Quick Post, or calendar view
    NOTE: If the user is asking to CREATE A DOCUMENT (ICP, GTM, etc.) that *mentions* social media strategy inside it, that is NOT redirect_to_scheduler — that is document work. Only redirect when the user wants to DO the posting/campaign, not write about it.

FLOW:
  continue           → Resume wherever we left off
  general_chat       → Doesn't fit anything above

══════════════════════════════════════════════════
HOW TO DECIDE
══════════════════════════════════════════════════

Follow these steps IN ORDER. Stop at the first step that gives you a clear answer.

STEP 1 — COMMANDS
These words/phrases ALWAYS map to the same action regardless of context:
  "generate" / "create it" / "build it" / "make it"  →  generate_document
  "show facts" / "view facts" / "my facts" / "what do you know"  →  show_facts
  "show progress" / "how am I doing"  →  show_progress
  "skip" / "next question"  →  skip_question
  "done" / "goodbye" / "bye" / "that's all"  →  done
  "what can you do"  →  list_documents
If the message matches one of these, use it. Otherwise continue to Step 2.

STEP 2 — WHAT DID THE SYSTEM JUST ASK?
Read LAST SYSTEM MESSAGE carefully. The system asked a question or made an offer. The user is almost certainly responding to it. Here's how to interpret responses for each situation:

WHEN SYSTEM ASKED A YES/NO QUESTION:
  Look at what "yes" means and what "no" means for THAT specific question.
  
  "Shall we begin?" / "Ready?"
    yes → start_questioning | no → list_documents

  "Would you like me to generate it now?"
    yes → generate_document | no → decline_generation

  "Is this correct?" (edit confirmation)
    yes → confirm_edit | no → cancel_edit

  "Would you like to edit anything else?"
    yes/edit X → edit_fact | no/continue/move on/let's go/done editing → continue

  "Would you like to continue where we left off?"
    yes/continue → continue | no/something else → list_documents

  "Would you like to work on another document?"
    yes → list_documents | no → done

  "Would you like me to save this as your answer, or type your own?"
    yes/save/sounds good/save this/save it → process_answer
    option A/option B/go with A/go with B/the first one/a/b/c → process_answer
    no/skip → skip_question

WHEN SYSTEM OFFERED CHOICES (switch/continue/explore):
  The system said something like "Switch to [DocA], continue with [DocB], or explore?"
    "switch" / "yes switch" / picks DocA by name  →  switch_document
    "continue" / "stay" / "keep going"  →  continue
    "explore" / "other options" / "what else"  →  list_documents
    simple "yes"  →  select_document (picks the inquired doc)
    simple "no"  →  reject_recommendation

WHEN SYSTEM ASKED A HARVESTER QUESTION (business question):
  The default assumption is the user is ANSWERING the question.
    Message contains business information (even partial) → process_answer
    "I don't know" / "idk" / "not sure" / "no idea" / "don't have" / "haven't thought about" → help_question
    "what do you mean" / "I don't understand" / "confused" / "help" / "huh" / "?" / "explain" / "clarify" → help_question
    "suggest" / "suggest something" / "give me ideas" / "give me options" / "help me answer" / "what should I say" / "give examples" / "can you help" → help_question
    "skip" / "next" / "pass" → skip_question
  IMPORTANT: If the system previously offered options (A/B/C) or suggested an answer,
  and the user picks one ("option A", "go with B", "the first one", "a", "save this",
  "save it", "yeah that works", "sounds good"), that counts as answering → process_answer.
  HOWEVER — if the user says something that is clearly NOT an answer:
    "switch" / "switch document" / "change document"  →  switch_document
    mentions a document name or code  →  inquire_document (set document)
    "show facts" / "edit" / "generate" / "done"  →  the matching command from Step 1
  When in doubt during questioning, choose process_answer — the extraction system will handle unclear input gracefully.

WHEN SYSTEM SHOWED DOCUMENT LIST:
  User picks a doc by name/code → select_document
  User asks about a doc ("what about X", "tell me about X") → inquire_document
  User asks for help choosing / "idk where to start" / "suggest something" / "recommend" / "help me pick" / "not sure which" → recommend_documents

WHEN SYSTEM RECOMMENDED DOCUMENTS:
  User picks one explicitly ("let's do ICP") → select_document
  User asks about one ("what about ICP", "how about ICP") → inquire_document
  User doesn't like them ("nah", "something else") → reject_recommendation
  User describes business instead → describe_business

WHEN SYSTEM SHOWED FACTS:
  User wants to edit ("change 3", "edit the name") → edit_fact
  User is done reviewing ("no", "continue", "looks good") → continue

WHEN SYSTEM ASKED WHICH FACT TO EDIT:
  User picks a fact → edit_fact
  User says nevermind → continue

WHEN SYSTEM ASKED FOR NEW VALUE:
  User provides a new value (any text) → edit_fact
  User says cancel/nevermind → cancel_edit

WHEN SYSTEM JUST COMPLETED AN EDIT (last_action = edit_completed):
  "Would you like to edit anything else?"
    "wait no" / "actually" / "wait" / "change it to" / "no change it to" → correct_edit (set "edit_value")
    "yes" / "edit X" / "change Y" → edit_fact
    "no" / "continue" / "let's go" / "done" / "nope" → continue

STEP 3 — DOCUMENT MENTIONS
If the user mentions a document by code (GTM, ICP, ICP2, MESSAGING, BRAND, MR, KMF, SR, SMP, BS) or by name, ALWAYS set the "document" field in your response.

To decide between select vs inquire vs switch:
  - COMMIT language ("let's do X", "I want X", "create X", "start X") → select_document
  - EXPLORE language ("what about X", "how about X", "tell me about X", "what's X") → inquire_document
  - CHANGE language ("switch to X", "change to X") → switch_document
  - If just a bare document name with no verb, and the system was asking "which document?": → select_document
  - If just a bare document name and system was NOT asking, default to → inquire_document

STEP 4 — CAMPAIGNS / SCHEDULER REDIRECT
  Detect if the user's INTENT is about executing, distributing, or scheduling content — not about creating a document.
  This covers a wide range of natural language expressions. Examples (non-exhaustive):
    • Direct: "I want to run a campaign", "schedule a LinkedIn post", "set up my content calendar"
    • Indirect: "how do I get my content out there?", "I need to start posting", "can you help me publish?"
    • Planning: "I want to plan my posts for next week", "what's my posting schedule look like?"
    • LinkedIn-specific: "connect my LinkedIn", "post this on LinkedIn", "LinkedIn outreach"
    • Campaign management: "check my campaigns", "edit my campaign", "how's my campaign doing?"
    • Content distribution: "distribute this content", "promote my article", "get this in front of people"
    • Tool references: "open the scheduler", "where's the calendar?", "quick post"
  If ANY of these intents are present → redirect_to_scheduler
  EXCEPTION: If the user is asking to create a DOCUMENT that discusses social/content strategy theoretically, that is document work, NOT a redirect.

STEP 5 — GREETINGS AND BUSINESS DESCRIPTIONS
  "hi" / "hello" / "hey" with no other content → greet
  Long message about their business with no document mention → describe_business
  "idk where to start" / "help me decide" / "suggest" / "recommend" → recommend_documents

STEP 6 — STILL UNSURE?
  If there's an active document and the system was mid-questioning → process_answer (let extraction handle it)
  Otherwise → general_chat

══════════════════════════════════════════════════
HARD RULES (these override everything above)
══════════════════════════════════════════════════

1. NEVER output "affirm", "deny", "yes", or "no" as actions. Always resolve to a concrete action.

2. "switch" ALWAYS means switch_document. It never means skip_question. It never means anything else. The word "switch" = switch_document, period.

3. "continue" means RESUME THE FLOW — go back to whatever we were doing (answering questions, or pick a doc if none selected). It maps to the "continue" action. It does NOT mean "yes I want to edit more" or "yes let's generate."

4. When last_action is "explained_document" and user says "switch" → switch_document. When user says "continue" → continue. These are responses to the three-way choice, not general commands.

5. If user says "generate" or "create it" at ANY point → generate_document. No gatekeeping.

6. cancel_generation is ONLY valid if the system was about to generate (last_action was ready_to_generate). In all other contexts, user rejecting something = reject_recommendation or decline_generation.

7. For edit flows: when last_action is "asked_new_value", the user's message IS the new value → edit_fact. Don't try to interpret it as anything else unless it's clearly a command like "cancel" or "nevermind".

══════════════════════════════════════════════════
RESPOND WITH JSON
══════════════════════════════════════════════════

{{
    "step_used": "which step (1-5) gave you the answer",
    "last_action_used": "{last_action}",
    "what_system_asked": "brief description of what the system was asking/offering",
    "user_intent": "brief description of what the user wants",
    "action": "action_name",
    "document": "DOC_CODE or null",
    "edit_target": "description or null",
    "edit_value": "new value or null",
    "reasoning": "one sentence"
}}"""



def unified_route(state: ConversationState) -> Dict[str, Any]:
    """Single LLM call that decides the action. Returns action dict."""
    lc = state.get('layer_context', {})
    last_action = lc.get('last_action', 'none')
    last_system_message = _get_last_system_message(state)
    state_summary = _build_state_summary(state)
    conversation_history = format_conversation_for_llm(state['conversation_history'], limit=6)

    formatted_prompt = UNIFIED_ROUTER_PROMPT.format(
        last_action=last_action,
        last_system_message=last_system_message[:800] if last_system_message else "(none — this is the first message in the conversation)",
        state_summary=state_summary,
        conversation_history=conversation_history if conversation_history else "(none — first message)"
    )

    context = {
        "user_message": state['user_message'],
        "last_action": last_action,
    }

    result = invoke_bedrock_json(formatted_prompt, json.dumps(context, default=str))

    if result and result.get('action'):
        print(f"🧠 UNIFIED ROUTER: action={result['action']} | short={result.get('is_short_response')} | affirm={result.get('is_affirmation')} | negate={result.get('is_negation')} | last_action_used={result.get('last_action_used')} | doc={result.get('document')} | {result.get('reasoning', '')}")
        return result

    print("⚠️ Unified router returned nothing, defaulting to general_chat")
    return {"action": "general_chat", "reasoning": "fallback — router returned empty"}



# ============================================================================
# CHANGE 3: LOOP BREAKER
# ============================================================================

def detect_loop(state: ConversationState) -> bool:
    """Check if the last 2 assistant messages suggest a loop."""
    history = state.get('conversation_history', [])
    assistant_msgs = [t['content'] for t in history if t.get('role') == 'assistant']

    if len(assistant_msgs) < 2:
        return False

    last_two = assistant_msgs[-2:]
    # Check if messages are very similar (same first 100 chars)
    if last_two[0][:100] == last_two[1][:100]:
        print("🔄 LOOP DETECTED: last 2 assistant messages are identical")
        return True

    # Check if last_action repeated 3+ times
    lc = state.get('layer_context', {})
    last_action = lc.get('last_action', '')
    if last_action and len(assistant_msgs) >= 3:
        # Count how many recent messages likely came from the same action
        # (rough heuristic: if the last 3 messages all start the same way)
        if last_two[0][:50] == last_two[1][:50]:
            print(f"🔄 LOOP DETECTED: repeated pattern with action={last_action}")
            return True

    return False


def break_loop(state: ConversationState) -> ConversationState:
    """Emergency loop breaker — uses LLM to generate a contextual escape."""
    system_prompt = """The conversation has gotten stuck in a loop. The user keeps getting the same response.

Your job: Generate ONE helpful response that breaks the pattern and moves the conversation forward.

RULES:
1. Acknowledge something feels off ("Let me try a different approach...")
2. Based on the state, pick the single most useful thing to do next
3. If they have an active document with questions remaining, ask the next question
4. If no document is selected, offer 2-3 specific recommendations
5. If document is ready, offer to generate
6. Keep it SHORT — max 3 sentences + one clear question
7. Do NOT repeat what the system already said

Respond with JSON:
{"response": "your loop-breaking response", "set_action": "optional action to set (like 'asked_question' or 'showed_doc_list')"}"""

    context = {
        "state_summary": _build_state_summary(state),
        "last_3_messages": format_conversation_for_llm(state['conversation_history'], limit=3),
        "user_message": state['user_message'],
        "available_documents": {code: desc['short'] for code, desc in DOCUMENT_DESCRIPTIONS.items() if code not in state.get('completed_documents', [])}
    }

    result = invoke_bedrock_json(system_prompt, json.dumps(context, default=str))

    if result and result.get('response'):
        state['response'] = result['response']
        action = result.get('set_action', 'loop_broken')
        state['layer_context'] = {'last_action': action}
    else:
        # Hard fallback
        state['response'] = (
            "Let me take a step back! 😊 Here's what we can do:\n\n"
            + build_document_list_text(state)
            + "\n\nWhich document would you like to work on?"
        )
        state['layer_context'] = {'last_action': 'loop_broken'}

    state['last_agent'] = 'loop_breaker'
    return state


# ============================================================================
# STICKY DISCOVERY / POST-GENERATION HANDLER
# ============================================================================

def handle_discovery_sticky(state: ConversationState) -> ConversationState:
    """Sticky handler for pre-document selection and post-generation phases.
    One LLM call handles: greeting, business description, document inquiry,
    recommendation, rejection, list, general chat, and post-gen guidance."""

    facts = state['facts']
    facts_count = len([f for f in facts.values() if f.get('value')])
    completed = state.get('completed_documents', [])
    lc = state.get('layer_context', {})
    conversation_history = format_conversation_for_llm(state.get('conversation_history', []), limit=10)
    business_profile = build_business_profile_summary(facts)
    total_user_messages = len([t for t in state.get('conversation_history', []) if t.get('role') == 'user'])

    # Build document list with descriptions
    doc_list_parts = []
    for code, desc in DOCUMENT_DESCRIPTIONS.items():
        if code in completed:
            doc_list_parts.append(f"{code}: {desc['name']} — {desc['short']} [COMPLETED]")
        else:
            readiness = calculate_document_readiness(code, facts)
            if readiness['is_ready']:
                doc_list_parts.append(f"{code}: {desc['name']} — {desc['short']} [READY to generate]")
            elif readiness['required_percentage'] > 0:
                doc_list_parts.append(f"{code}: {desc['name']} — {desc['short']} [{readiness['required_percentage']:.0f}% complete]")
            else:
                doc_list_parts.append(f"{code}: {desc['name']} — {desc['short']}")
    document_list_str = "\n".join(doc_list_parts)

    # Build progression context for post-generation
    progression_context = ""
    last_generated = lc.get('generated_doc')
    if last_generated and last_generated in DOCUMENT_PROGRESSION:
        prog = DOCUMENT_PROGRESSION[last_generated]
        next_docs = []
        for next_code in prog.get('natural_next', []):
            if next_code not in completed and next_code in DOCUMENT_DESCRIPTIONS:
                reasoning = prog.get('reasoning', {}).get(next_code, '')
                next_docs.append(f"- {next_code}: {DOCUMENT_DESCRIPTIONS[next_code]['name']} — {reasoning}")
        if next_docs:
            progression_context = "JUST COMPLETED: {} ({})\nRECOMMENDED NEXT STEPS:\n{}".format(
                DOCUMENT_REQUIREMENTS[last_generated]['name'] if last_generated in DOCUMENT_REQUIREMENTS else last_generated,
                last_generated,
                "\n".join(next_docs)
            )

    # Priority facts for opportunistic extraction
    priority_facts = [
        'business.name', 'business.description_short', 'business.description_long',
        'business.industry', 'business.stage', 'business.business_model',
        'product.core_offering', 'product.type', 'product.problems_solved',
        'product.value_proposition_short', 'product.unique_differentiation',
        'customer.primary_customer', 'customer.industries', 'customer.company_size',
        'customer.geography'
    ]
    existing_fact_ids = {k for k, v in facts.items() if v.get('value')}
    missing_facts = {k: FACT_UNIVERSE[k] for k in priority_facts if k not in existing_fact_ids}
    missing_facts_str = json.dumps(missing_facts) if missing_facts else "{}"

    # Determine user phase
    is_post_generation = state.get('active_layer') == LAYER_POST_GENERATION
    is_new_user = (total_user_messages == 0 and not completed)
    is_returning_user = (total_user_messages == 0 and len(completed) > 0)

    system_prompt = """You are Cammi's document creation module — a friendly, conversational assistant that helps users create professional business documents. You are warm, helpful, and guide users naturally without being pushy.

""" + SCHEDULER_KNOWLEDGE + """

CURRENT STATE:
- User phase: {user_phase}
- Facts collected: {facts_count}
- Business profile: {business_profile}
- Completed documents: {completed_docs}
- Conversation messages so far: {msg_count}
{progression_section}

ALL AVAILABLE DOCUMENTS:
{document_list}

RECENT CONVERSATION:
{conversation_history}

EXTRACTABLE FACTS (if user shares business info):
{missing_facts}

YOUR BEHAVIOUR:

FIRST MESSAGE (no conversation history):
- If new user: Give a punchy intro (no more than 5 sentences total):
  1. One-line intro: you're Cammi's document assistant.
  2. A quick teaser of what you create (mention 4-5 specific document types by name, e.g. Ideal Customer Profile, Messaging Framework, Brand Strategy, GTM Plan, Market Research).
  3. One line on how it works: you ask targeted questions, gather info, generate a tailored document — no templates, no generic filler.
  4. End with one direct question: Check the 'Facts collected' array. If you already have business info (like business description or model), incorporate what you know into the sentence and ask "To help us pick the best document to start with, what is your biggest marketing focus right now? (e.g. finding buyers, refining your brand, launching a new service, etc.)". If you DO NOT have business info, ask "To help us pick the right place to start, I just need a little bit of context. First up — what does your company do?".
  Do NOT offer passive choices. Do NOT dump the full list. Be confident and energetic, not corporate.
- If returning user: Welcome them back. Mention what you know about their business and what documents they have completed. Ask what they would like to work on next.

ONGOING CONVERSATION:
Read the conversation history and the user's message. Determine what they want:

1. SELECTING A DOCUMENT: If the user commits to creating a specific document (e.g. "let's do ICP", "I want GTM", "start with market research"), set next_action="select_document" and document to the CODE. Your response should confirm the choice enthusiastically.

2. ASKING ABOUT A DOCUMENT: If the user wants to learn about a document before committing (e.g. "what is ICP?", "tell me about GTM", "what does market research include?"), explain it in your response using the document list above (what it is, what it does, why it matters). Then softly ask if they want to create it. Stay in discovery (next_action="stay").

3. SHARING BUSINESS INFO: If the user describes their business, extract facts into extracted_facts using the extractable facts list. Acknowledge what they shared in one sentence. Then recommend 2-3 specific documents that fit their situation — be direct and confident, e.g. "Based on that, here's where I'd start:". Give a brief reason for each recommendation. Do NOT show the full document list unless they explicitly ask. Do NOT extract from questions, hypotheticals, or examples about other companies.

4. ASKING FOR RECOMMENDATIONS: If the user asks "where should I start?", "what do you recommend?", "help me choose" — recommend 1-2 documents with brief reasoning based on their business profile (or general recommendations if you know nothing about them yet).

5. REJECTING A SUGGESTION: If the user says no to your recommendation, acknowledge warmly and suggest different documents. Do not be pushy.

6. ASKING TO SEE ALL DOCUMENTS: If user asks to see all options / what documents are available, include the document list in your response in a clean format.

7. POST-GENERATION GUIDANCE: If we just completed a document, proactively celebrate and suggest the recommended next steps to create a NEW document. IMPORTANT: The user can already view the completed document in their dashboard or chat. DO NOT offer to review, discuss, summarize, or output the generated document in the chat. Provide ONLY options to create the next logical document or ask what new strategy they'd like to work on.

8. GENERAL/OFF-TOPIC: If the user says something casual or off-topic, be friendly and brief (1 sentence), then gently steer back. After 2-3 general messages, be more direct about guiding them toward picking a document.

9. DONE: If the user says goodbye, farewell, done — set next_action="done".

10. VIEWING COLLECTED INFO: If the user wants to see what information has been collected about their business (e.g. "show me what you have", "what info do you have", "my business info", "show my details", "what have you collected"), set next_action="show_facts". Do NOT try to display the facts yourself — the system will handle it.

11. EDITING COLLECTED INFO: If the user wants to edit or change collected information (e.g. "I want to change something", "update my info", "fix something", "correct my details"), set next_action="edit_fact". Do NOT try to handle the edit yourself — the system will handle it.

12. CAMPAIGNS / SCHEDULING / CONTENT DISTRIBUTION: If the user's intent is about EXECUTING, DISTRIBUTING, or SCHEDULING content rather than creating a document, set next_action="redirect_to_scheduler". This includes but is not limited to:
   - Creating, running, managing, launching, pausing, or tracking campaigns
   - Scheduling, publishing, or planning posts (LinkedIn or social media generally)
   - Content calendar management (viewing, planning, organising a posting timeline)
   - LinkedIn-specific actions (connecting LinkedIn, posting on LinkedIn, LinkedIn outreach)
   - Content promotion, distribution, audience engagement via posts
   - References to the Scheduler tool, Quick Post, or calendar
   - Indirect phrasings like "how do I get my content out there?", "I need to start posting", "can you help me publish?", "plan my posts for next week"
   Acknowledge their interest warmly and let them know this is handled by CAMMI's Scheduler module. Do NOT try to handle campaigns yourself.
   EXCEPTION: If they want to create a DOCUMENT that discusses social/content strategy theoretically, that is document work — do NOT redirect.

IMPORTANT RULES:
- Be conversational and natural, like talking to a friend who is helping you with business.
- Do NOT dump the full document list unless the user asks for it.
- When recommending, explain WHY a document fits their situation.
- Keep responses concise: 3-5 sentences for most cases.
- When the user mentions a document by code or name, use the document list to map to the correct CODE.
- For fact extraction: ONLY extract from first-person statements about THEIR business. Never from questions or examples.

Respond with valid JSON only:
{{"response": "your conversational message", "next_action": "stay" | "select_document" | "show_facts" | "edit_fact" | "redirect_to_scheduler" | "done", "document": "CODE or null", "extracted_facts": {{"fact.id": "value"}} }}"""

    # Build user phase description
    if is_new_user:
        user_phase = "Brand new user, first message ever. No prior conversation."
    elif is_returning_user:
        user_phase = "Returning user (first message this session), has prior data."
    elif is_post_generation:
        gen_doc = lc.get('generated_doc_name', lc.get('generated_doc', 'a document'))
        user_phase = f"Just generated {gen_doc}. In post-generation mode."
    else:
        user_phase = "In discovery mode, exploring options."

    formatted_prompt = system_prompt.format(
        user_phase=user_phase,
        facts_count=facts_count,
        business_profile=business_profile,
        completed_docs=", ".join(completed) if completed else "none",
        msg_count=total_user_messages,
        progression_section=progression_context if progression_context else "",
        document_list=document_list_str,
        conversation_history=conversation_history if conversation_history else "(no prior messages — this is the very first message)",
        missing_facts=missing_facts_str,
    )

    context = {
        "user_message": state['user_message'],
    }

    try:
        result = invoke_bedrock_json(formatted_prompt, json.dumps(context, default=str))
    except Exception as e:
        print(f"❌ Discovery sticky LLM error: {e}")
        # Build dynamic fallback greeting
        has_business_desc = any(
            state.get('facts', {}).get(f, {}).get('value') 
            for f in ['business.description_short', 'business.description_long', 'business.business_model']
        )
        if has_business_desc:
            business_name = state.get('facts', {}).get('business.name', {}).get('value', '').strip()
            desc = state.get('facts', {}).get('business.description_short', {}).get('value') or \
                   state.get('facts', {}).get('business.business_model', {}).get('value') or \
                   state.get('facts', {}).get('business.description_long', {}).get('value') or ''
            
            if business_name and desc:
                phrase = f"**{business_name}** — **{desc}**"
            elif business_name:
                phrase = f"**{business_name}**"
            elif desc:
                phrase = f"a **{desc}**"
            else:
                phrase = "**your business**"

            fallback_msg = (
                "Hey! 👋 I'm Cammi's document assistant.\n\n"
                "I help businesses build things like Ideal Customer Profiles, Messaging Frameworks, Brand Strategy, GTM Plans, Market Research, and more.\n\n"
                f"I see you're building {phrase}!\n\n"
                "**To help us pick the best document to start with, what is your biggest marketing focus right now?** (e.g. finding buyers, refining your brand, launching a new service, etc.)"
            )
        else:
            fallback_msg = (
                "Hey! 👋 I'm Cammi's document assistant.\n\n"
                "I help businesses build things like Ideal Customer Profiles, Messaging Frameworks, Brand Strategy, GTM Plans, Market Research, and more.\n\n"
                "How it works: I ask you a few targeted questions, gather the right info, and generate a tailored document.\n\n"
                "To help us pick the right place to start, I just need a little bit of context.\n\n"
                "**First up — what does your company do?**"
            )
            
        state['response'] = fallback_msg
        state['layer_context'] = {'last_action': 'greeted_new'}
        state['last_agent'] = 'discovery_sticky'
        return state

    if not result or not result.get('response'):
        # Build dynamic fallback greeting
        has_business_desc = any(
            state.get('facts', {}).get(f, {}).get('value') 
            for f in ['business.description_short', 'business.description_long', 'business.business_model']
        )
        if has_business_desc:
            business_name = state.get('facts', {}).get('business.name', {}).get('value', '').strip()
            desc = state.get('facts', {}).get('business.description_short', {}).get('value') or \
                   state.get('facts', {}).get('business.business_model', {}).get('value') or \
                   state.get('facts', {}).get('business.description_long', {}).get('value') or ''
            
            if business_name and desc:
                phrase = f"**{business_name}** — **{desc}**"
            elif business_name:
                phrase = f"**{business_name}**"
            elif desc:
                phrase = f"a **{desc}**"
            else:
                phrase = "**your business**"

            fallback_msg = (
                "Hey! 👋 I'm Cammi's document assistant.\n\n"
                "I help businesses build things like Ideal Customer Profiles, Messaging Frameworks, Brand Strategy, GTM Plans, Market Research, and more.\n\n"
                f"I see you're building {phrase}!\n\n"
                "**To help us pick the best document to start with, what is your biggest marketing focus right now?** (e.g. finding buyers, refining your brand, launching a new service, etc.)"
            )
        else:
            fallback_msg = (
                "Hey! 👋 I'm Cammi's document assistant.\n\n"
                "I help businesses build things like Ideal Customer Profiles, Messaging Frameworks, Brand Strategy, GTM Plans, Market Research, and more.\n\n"
                "How it works: I ask you a few targeted questions, gather the right info, and generate a tailored document.\n\n"
                "To help us pick the right place to start, I just need a little bit of context.\n\n"
                "**First up — what does your company do?**"
            )
            
        state['response'] = fallback_msg
        state['layer_context'] = {'last_action': 'greeted_new'}
        state['last_agent'] = 'discovery_sticky'
        return state
   
    response_text = result.get('response', '')
    next_action = result.get('next_action', 'stay')
    document = result.get('document')
    extracted_facts = result.get('extracted_facts', {}) or {}

    print(f"🔄 DISCOVERY STICKY: next_action={next_action}, doc={document}, facts={len(extracted_facts)}, phase={user_phase}")

    # Save extracted facts
    if extracted_facts:
        facts_to_save = {}
        for k, v in extracted_facts.items():
            if v and str(v).strip() and k in FACT_UNIVERSE:
                facts_to_save[k] = str(v).strip()
        if facts_to_save:
            saved_count = save_multiple_facts(state['project_id'], facts_to_save, 'chat')
            print(f"💾 Saved {saved_count} facts from discovery_sticky: {list(facts_to_save.keys())}")
            for fid, value in facts_to_save.items():
                state['facts'][fid] = {
                    'value': value,
                    'source': 'chat',
                    'updated_at': datetime.utcnow().isoformat()
                }

    # Route based on next_action
    if next_action == 'select_document' and document and document in DOCUMENT_REQUIREMENTS:
        state['response'] = response_text
        state['last_agent'] = 'discovery_sticky'
        return handle_select_document(state, {'document': document})

    if next_action == 'show_facts':
        state['last_agent'] = 'discovery_sticky'
        return handle_show_facts(state, {})

    if next_action == 'edit_fact':
        state['last_agent'] = 'discovery_sticky'
        return handle_edit_fact(state, {})

    if next_action == 'done':
        state['response'] = response_text
        state['last_agent'] = 'discovery_sticky'
        return handle_done(state, {})

    if next_action == 'redirect_to_scheduler':
        state['last_agent'] = 'discovery_sticky'
        return handle_redirect_to_scheduler(state, {})

    # Default: stay in discovery/post-gen
    state['response'] = response_text
    state['active_layer'] = state.get('active_layer', LAYER_DISCOVERY)
    state['layer_context'] = {
        'last_action': 'discovery_chat',
        'generated_doc': lc.get('generated_doc'),
        'generated_doc_name': lc.get('generated_doc_name'),
    }
    state['last_agent'] = 'discovery_sticky'
    return state


# ============================================================================
# ACTION HANDLERS — each action maps to a clean, focused function
# ============================================================================

def handle_greet(state: ConversationState, action: Dict) -> ConversationState:
    facts = state['facts']
    facts_count = len([f for f in facts.values() if f.get('value')])
    completed = state.get('completed_documents', [])
    total_user_messages = len([t for t in state.get('conversation_history', []) if t.get('role') == 'user'])

    # A new user is identified by: no conversation history AND no completed docs
    # (facts may already exist from webapp onboarding, so they don't determine new vs returning)
    is_new = (total_user_messages == 0 and not completed)

    if not is_new and state.get('active_document'):
        doc_name = get_document_display_name(state['active_document'])
        readiness = calculate_document_readiness(state['active_document'], facts)
        state['response'] = (
            f"Welcome back! 👋\n\nI see we were working on your **{doc_name}** "
            f"({readiness['required_percentage']:.0f}% complete). Would you like to "
            f"continue where we left off, or do something else?"
        )
        state['layer_context'] = {'last_action': 'greeted_returning'}
    elif not is_new:
        business_name = facts.get('business.name', {}).get('value', 'your business')
        state['response'] = (
            f"Welcome back! 👋 I have some information about {business_name} saved.\n\n"
            f"What would you like to work on today?\n\n{build_document_list_text(state)}"
        )
        state['layer_context'] = {'last_action': 'greeted_returning'}
    else:
        # Check if we already have facts that describe what the company does
        has_business_desc = any(
            facts.get(f, {}).get('value') for f in ['business.description_short', 'business.description_long', 'business.business_model']
        )

        if has_business_desc:
            # User has pre-filled info from webapp onboarding — skip the basics, jump into the journey
            business_name = facts.get('business.name', {}).get('value', '').strip()
            desc = facts.get('business.description_short', {}).get('value') or \
                   facts.get('business.business_model', {}).get('value') or \
                   facts.get('business.description_long', {}).get('value') or ''

            if business_name and desc:
                name_phrase = f"**{business_name}** ({desc})"
            elif business_name:
                name_phrase = f"**{business_name}**"
            elif desc:
                name_phrase = f"your business ({desc})"
            else:
                name_phrase = "your business"

            intro_text = (
                "Hey! 👋 Welcome to your **Clarify Journey**.\n\n"
                f"I already have some details about {name_phrase} from your profile, so we can hit the ground running.\n\n"
                "This is where we build your marketing foundation together — up to 10 strategy documents covering your Ideal Customer Profile, "
                "Go-To-Market Plan, Brand Strategy, Market Research, and more. "
                "I'll use what I already know about your business and ask follow-up questions as we go, "
                "so every document ends up tailored specifically to you.\n\n"
                " Complete any 2 Clarify documents to unlock the **Align** section and keep moving forward.\n\n"
                "You don't have to do it all at once — come back anytime and we'll pick up right where you left off.\n\n"
                "**What's your biggest marketing focus right now?** (e.g. finding the right customers, nailing your messaging, planning your launch, etc.)"
            )
        else:
            # Completely new user — no info at all
            intro_text = (
                "Hey! 👋\n\n"
                "I'm **Cammi** — your AI marketing agent.\n\n"
                "Welcome to the **Clarify** Section. This is where we turn your business ideas into a clear marketing foundation through key strategy documents like your **Ideal Customer Profile**, **Go-to-Market Plan**, and **Market Research** \n\n"
                "At the top, you’ll see all the documents in this section. Click any name or tell me in chat to start. Not sure what to pick? We’ll figure it out together.\n\n Complete any 2 Clarify documents to unlock the **Align** section and keep moving forward.\n\n"
                "You don't have to do it all at once — come back anytime, and we'll pick up right where you left off.\n\n"
                "Let's start from the beginning — **what does your company do?**"
            )

        state['response'] = intro_text
        state['layer_context'] = {'last_action': 'greeted_new'}

    state['active_layer'] = LAYER_DISCOVERY
    return state


def handle_describe_business(state: ConversationState, action: Dict) -> ConversationState:
    extracted = extract_facts_opportunistically(state, state['user_message'])
    completed = state.get('completed_documents', [])
    profile = build_business_profile_summary(state['facts'])

    system_prompt = """The user just described their business. Respond warmly.

1. Acknowledge what they shared (reference specifics)
2. If facts were extracted, briefly confirm what you understood  
3. Recommend 1-2 documents from AVAILABLE list that fit their business
4. Ask which they'd like to create

AVAILABLE DOCUMENTS (only recommend from these — user has NOT completed these):
{available_docs}

COMPLETED (do NOT recommend): {completed}

Keep it concise — 3-4 sentences + recommendations. Be warm.

Respond with JSON:
{{"response": "your response"}}"""

    # Provide richer (but token-capped) document context so recommendations are more accurate.
    # We include short + a trimmed description to reduce latency/cost vs sending full text.
    available = {}
    for code, desc in DOCUMENT_DESCRIPTIONS.items():
        if code in completed:
            continue
        full_desc = (desc.get('description') or '').strip()
        trimmed_desc = full_desc[:220] + ('…' if len(full_desc) > 220 else '')
        available[code] = {
            "name": desc.get('name'),
            "short": desc.get('short'),
            "description": trimmed_desc
        }

    result = invoke_bedrock_json(
        system_prompt.format(
            available_docs=json.dumps(available),
            completed=', '.join(completed) if completed else 'none'
        ),
        json.dumps({
            "user_message": state['user_message'],
            "business_profile": profile,
            "facts_extracted": extracted
        }, default=str)
    )

    if result and result.get('response'):
        state['response'] = result['response']
    else:
        state['response'] = (
            f"Thanks for sharing! I've noted that down. "
            f"Would you like to create a document? Here are your options:\n\n"
            + build_document_list_text(state)
        )

    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'received_business_description'}
    return state


def handle_select_document(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = action.get('document')

    # Try to extract document if not provided
    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        doc_code = _extract_document_from_message(state['user_message'])

    # Check layer_context for inquired_doc if we still don't have a doc
    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        lc = state.get('layer_context', {})
        inquired = lc.get('inquired_doc')
        if inquired and inquired in DOCUMENT_REQUIREMENTS:
            doc_code = inquired

    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        state['response'] = (
            f"I'd love to help! Which document would you like to create?\n\n"
            + build_document_list_text(state) + "\n\nJust tell me the name or code!"
        )
        state['layer_context'] = {'last_action': 'asked_which_document'}
        state['active_layer'] = LAYER_DISCOVERY
        return state

    # Check section and update current_tab accordingly
    if doc_code in ALIGN_DOCUMENTS:
        eligibility = check_align_eligibility(state['completed_documents'])
        if not eligibility['is_eligible']:
            doc_name = get_document_display_name(doc_code)
            clarify_count = eligibility['clarify_count']
            min_required = eligibility['min_required']
            state['response'] = (
                f"I'd love to help you with the **{doc_name}**, but you'll need to complete "
                f"at least {min_required} Clarify documents first. "
                f"You've completed {clarify_count} so far.\n\n"
                f"Once you hit {min_required}, you'll unlock the Align section!"
            )
            state['layer_context'] = {'last_action': 'blocked_align_access'}
            state['active_layer'] = LAYER_DISCOVERY
            return state
        else:
            # User is eligible - switch to Align section via specific document selection
            # Mark align_intro_shown=True since the user is entering Align with a specific doc
            state['current_tab'] = SECTION_ALIGN
            state['align_intro_shown'] = True
            update_section_state(state['project_id'], SECTION_ALIGN, state['completed_documents'], align_intro_shown=True)
    elif doc_code in CLARIFY_DOCUMENTS and state.get('current_tab') != SECTION_CLARIFY:
        # User is in Align but selected a Clarify document — switch back to Clarify
        state['current_tab'] = SECTION_CLARIFY
        update_section_state(state['project_id'], SECTION_CLARIFY, state['completed_documents'], align_intro_shown=state.get('align_intro_shown', False))

    facts = state['facts']
    state['active_document'] = doc_code
    state['generating_document'] = None
    state['interrupted_document'] = None
    state['current_question_id'] = None
    state['question_attempts'] = {}
    state['pending_questions'] = determine_pending_questions(
        doc_code, facts, state['asked_questions'], state.get('skipped_questions', [])
    )

    doc_name = get_document_display_name(doc_code)
    readiness = calculate_document_readiness(doc_code, facts)

    if readiness['is_ready']:
        state['response'] = (
            f"Great choice! Let's work on your **{doc_name}**.\n\n"
            f"I already have all the required information! "
            f"Would you like me to generate it now, or review the details first?"
        )
        state['active_layer'] = LAYER_GENERATION
        state['layer_context'] = {'last_action': 'ready_to_generate', 'sticky_general_chat': False}
    elif state['pending_questions']:
        # Go straight to first question: acknowledgment + help line + question (no "Ready?" / "Shall we begin?" step)
        first_q_id = state['pending_questions'][0]
        first_q_text = GLOBAL_HARVESTERS[first_q_id]['question']
        state['current_question_id'] = first_q_id
        attempts = state.get('question_attempts', {})
        attempts[first_q_id] = 1
        state['question_attempts'] = attempts

        # Check if this is the user's very first document ever (onboarding guide)
        is_first_doc_ever = (
            not state.get('completed_documents')
            and not state.get('asked_questions')
        )

        if is_first_doc_ever:
            ack = (
                f"**{doc_name}** — great pick! Here's how this works:\n\n"
                f"I'll ask you a few questions to build your document. You don't need to overthink it:\n"
                f"- **Skip** any question you're not sure about\n"
                f"- Say **help** and I'll suggest answers for you\n"
                f"- Say **generate** anytime to create your document with what we have\n\n"
                f"The more you share, the better the document — but you can always come back and add more later."
            )
        elif readiness['required_percentage'] > 0:
            ack = (
                f"Got it — we're on your **{doc_name}**. "
                f"I already have {readiness['required_percentage']:.0f}% of what I need. "
                f"I'm here if you'd like any help."
            )
        else:
            ack = (
                f"**{doc_name}** it is — I'm here to help if you need it."
            )

        state['response'] = f"{ack}\n\n**{first_q_text}**"
        state['active_layer'] = LAYER_QUESTIONING
        state['layer_context'] = {'last_action': 'asked_question', 'question_id': first_q_id, 'sticky_general_chat': False}
    else:
        # No pending questions but not ready (edge case): offer to generate with inferred gaps
        state['response'] = (
            f"Got it — **{doc_name}** it is. "
            f"I'm here to help if you need it.\n\n"
            f"I can generate it now; I'll fill in any gaps. Would you like to proceed?"
        )
        state['active_layer'] = LAYER_GENERATION
        state['layer_context'] = {'last_action': 'ready_to_generate', 'sticky_general_chat': False}

    return state


def handle_inquire_document(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = action.get('document')
    if not doc_code or doc_code not in DOCUMENT_DESCRIPTIONS:
        doc_code = _extract_document_from_message(state['user_message'])

    if not doc_code or doc_code not in DOCUMENT_DESCRIPTIONS:
        state['response'] = (
            f"I'd be happy to tell you about any document!\n\n"
            + build_document_list_text(state)
        )
        state['layer_context'] = {'last_action': 'asked_which_document'}
        state['active_layer'] = LAYER_DISCOVERY
        return state

    desc = DOCUMENT_DESCRIPTIONS[doc_code]
    facts = state['facts']
    profile = build_business_profile_summary(facts)
    readiness = calculate_document_readiness(doc_code, facts)
    active_doc = state.get('active_document')

    # Check if this is an Align document and handle eligibility
    if doc_code in ALIGN_DOCUMENTS:
        eligibility = check_align_eligibility(state['completed_documents'])
        doc_name = get_document_display_name(doc_code)

        if not eligibility['is_eligible']:
            # Not eligible - explain requirement
            clarify_count = eligibility['clarify_count']
            min_required = eligibility['min_required']
            state['response'] = (
                f"The **{doc_name}** is part of the Align section, which helps you coordinate your team "
                f"and align everyone around your strategy.\n\n"
                f"To access Align documents, you'll need to complete at least {min_required} Clarify documents first. "
                f"You've completed {clarify_count} so far — just {min_required - clarify_count} more to go!"
            )
            state['layer_context'] = {'last_action': 'explained_align_requirement', 'inquired_doc': doc_code}
            state['active_layer'] = LAYER_DISCOVERY
            return state
        else:
            # Eligible - offer to switch to Align and create the document
            current_tab = state.get('current_tab', SECTION_CLARIFY)
            if current_tab == SECTION_CLARIFY:
                # User is in Clarify but eligible for Align - explain and offer switch
                if readiness['is_ready']:
                    readiness_note = "I already have all the info needed to create it."
                elif readiness['required_percentage'] > 0:
                    readiness_note = f"I have {readiness['required_percentage']:.0f}% of the info — we'd just need to answer a few questions."
                else:
                    readiness_note = "We'd need to gather some information first."

                state['response'] = (
                    f"The **{doc_name}** is one of the Align documents — it helps {desc['description'].lower()}\n\n"
                    f"{readiness_note}\n\n"
                    f"Would you like to switch to the Align section and create your {doc_name}?"
                )
                state['layer_context'] = {'last_action': 'offered_align_switch', 'inquired_doc': doc_code}
                state['active_layer'] = LAYER_DISCOVERY
                return state

    # Build readiness context for the LLM
    if readiness['is_ready']:
        readiness_note = "ALL required facts are collected — this document is ready to generate immediately."
    elif readiness['required_percentage'] > 0:
        readiness_note = f"{readiness['required_percentage']:.0f}% of required info is already collected. A few more questions would be needed."
    else:
        readiness_note = "We'd need to gather information through some questions first."

    # Determine if there's an active document that's different from the inquired one
    has_different_active = active_doc and active_doc != doc_code and active_doc in DOCUMENT_DESCRIPTIONS
    active_doc_name = get_document_display_name(active_doc) if has_different_active else None
    inquired_doc_name = desc['name']

    if has_different_active:
        closing_instruction = (
            f"IMPORTANT — End your response with EXACTLY this kind of closing (adapt naturally but keep all three options):\n"
            f"\"Would you like to switch to {inquired_doc_name}, continue with {active_doc_name}, or explore other options?\"\n"
            f"You MUST mention BOTH document names explicitly. Do NOT say 'this one' or 'that one'."
        )
    else:
        closing_instruction = (
            f"End with a soft offer like: \"Want to go ahead and create your {inquired_doc_name}, or would you like to explore other options?\""
        )

    system_prompt = """Explain this document AND offer to create it — all in one response. Be warm and conversational.

STRUCTURE YOUR RESPONSE LIKE THIS:
1. What the document IS and DOES (2-3 simple sentences)
2. If you know their business, explain how it helps THEM specifically
3. Mention readiness status naturally (e.g., "Good news — I already have most of what's needed!" or "We'd need to answer a few questions first.")
4. {closing_instruction}

IMPORTANT:
- Keep it concise: 4-6 sentences total
- Do NOT just list features — explain the VALUE
- The closing question MUST name specific documents — NEVER use "this one" or "that one"

Respond with JSON: {{"response": "your explanation + offer"}}"""

    result = invoke_bedrock_json(
        system_prompt.format(closing_instruction=closing_instruction),
        json.dumps({
            "document_code": doc_code,
            "document_name": inquired_doc_name,
            "document_description": desc['description'],
            "business_profile": profile,
            "readiness_note": readiness_note,
            "readiness_percentage": readiness['required_percentage'],
            "is_ready": readiness['is_ready'],
            "has_active_document": has_different_active,
            "active_document_name": active_doc_name,
            "active_document_code": active_doc if has_different_active else None
        }, default=str)
    )

    if result and result.get('response'):
        state['response'] = result['response']
    else:
        # Fallback with explicit naming built in
        ready_msg = ""
        if readiness['is_ready']:
            ready_msg = " Great news — I already have all the info needed to generate it!"
        elif readiness['required_percentage'] > 0:
            ready_msg = f" I already have {readiness['required_percentage']:.0f}% of what's needed."

        if has_different_active:
            closing = (
                f"Would you like to switch to **{inquired_doc_name}**, "
                f"continue with **{active_doc_name}**, or explore other options?"
            )
        else:
            closing = (
                f"Want to go ahead and create your **{inquired_doc_name}**, "
                f"or explore other options?"
            )

        state['response'] = (
            f"**{inquired_doc_name}** ({doc_code})\n\n"
            f"{desc['description']}{ready_msg}\n\n{closing}"
        )

    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {
        'last_action': 'explained_document',
        'explained_doc': doc_code,
        'inquired_doc': doc_code
    }
    return state


def handle_recommend_documents(state: ConversationState, action: Dict) -> ConversationState:
    facts = state['facts']
    completed = state.get('completed_documents', [])
    facts_count = len([f for f in facts.values() if f.get('value')])

    system_prompt = """Recommend 1-2 documents for the user. Be warm and specific.

VALID CODES: GTM, ICP, ICP2, MESSAGING, BRAND, MR, KMF, SR, SMP, BS
DO NOT recommend completed documents: {completed}

Rules:
1. If they're new with no info, suggest ICP or GTM
2. If they described their business, match to their needs
3. If they completed docs, suggest natural progression
4. Explain WHY each recommendation fits THEM
5. End with a clear question

Respond with JSON:
{{"response": "your recommendation", "recommended_docs": ["DOC1", "DOC2"]}}"""

    available = {code: desc['short'] for code, desc in DOCUMENT_DESCRIPTIONS.items() if code not in completed}

    result = invoke_bedrock_json(
        system_prompt.format(completed=', '.join(completed) if completed else 'none'),
        json.dumps({
            "user_message": state['user_message'],
            "business_profile": build_business_profile_summary(facts),
            "facts_count": facts_count,
            "completed_documents": completed,
            "available_documents": available,
            "progression": {doc: DOCUMENT_PROGRESSION.get(doc, {}).get('natural_next', []) for doc in completed}
        }, default=str)
    )

    if result and result.get('response'):
        state['response'] = result['response']
    elif facts_count == 0:
        state['response'] = (
            "I'd recommend starting by telling me about your business — what you do and who you serve. "
            "That helps me point you to the right document.\n\n"
            "Or jump in with **Ideal Customer Profile (ICP)** or **Go-to-Market (GTM)** — both are great starting points!"
        )
    else:
        state['response'] = f"Based on your business, here are my suggestions:\n\n{build_document_list_text(state)}\n\nWhich interests you?"

    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'gave_recommendation'}
    return state

def handle_reject_recommendation(state: ConversationState, action: Dict) -> ConversationState:
    """User rejected a document recommendation — acknowledge and suggest alternatives."""
    facts = state['facts']
    completed = state.get('completed_documents', [])

    # Figure out what was recommended (from last system message)
    last_msg = _get_last_system_message(state)

    # Build list of all available docs excluding completed
    available = {code: desc for code, desc in DOCUMENT_DESCRIPTIONS.items() if code not in completed}

    system_prompt = """The user just rejected your document recommendation. Respond naturally.

RULES:
1. Acknowledge their preference warmly — don't be pushy ("No problem!" or "Fair enough!")
2. Look at what you recommended before (in LAST_MESSAGE) and suggest DIFFERENT documents from AVAILABLE_DOCS
3. Pick 2-3 alternatives and briefly explain why each might be a good fit for their business
4. If you know their business context, tailor the suggestions
5. End with a simple question: which of these sounds better, or would they like to hear about something else
6. Keep it to 4-5 sentences max. Don't dump the entire doc list.
7. Do NOT re-recommend what was just rejected

AVAILABLE DOCUMENTS (pick from these only):
{available_docs}

Respond with JSON: {{"response": "your response"}}"""

    result = invoke_bedrock_json(
        system_prompt.format(available_docs=json.dumps({code: desc['short'] for code, desc in available.items()})),
        json.dumps({
            "user_message": state['user_message'],
            "last_system_message": last_msg[:500] if last_msg else "",
            "business_profile": build_business_profile_summary(facts),
            "completed_documents": completed
        }, default=str)
    )

    if result and result.get('response'):
        state['response'] = result['response']
    else:
        # Fallback: show list with a soft intro
        state['response'] = (
            f"No problem! Here are some other options that might be a better fit:\n\n"
            + build_document_list_text(state)
            + "\n\nAnything catch your eye?"
        )

    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'gave_recommendation'}
    return state

def handle_list_documents(state: ConversationState, action: Dict) -> ConversationState:
    completed = state.get('completed_documents', [])

    if len(completed) == len(DOCUMENT_REQUIREMENTS):
        state['response'] = (
            "🎉 You've completed all available documents!\n\n"
            "You can review info, edit facts, or regenerate any document. What would you like?"
        )
    elif completed:
        completed_names = [get_document_display_name(d) for d in completed]
        state['response'] = (
            f"You've completed: {', '.join(completed_names)}.\n\n"
            f"Here are the remaining documents:\n\n{build_document_list_text(state)}\n\nWhich one interests you?"
        )
    else:
        state['response'] = f"Here are all the documents I can create:\n\n{build_document_list_text(state)}\n\nWhich would you like?"

    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'showed_doc_list'}
    return state


def handle_show_progress(state: ConversationState, action: Dict) -> ConversationState:
    facts = state['facts']
    completed = state.get('completed_documents', [])
    total_facts = len([f for f in facts.values() if f.get('value')])

    response = f"📊 **Your Progress:**\n\n**{total_facts} facts** collected\n\n"
    if completed:
        response += f"**Completed:** {', '.join([get_document_display_name(d) for d in completed])}\n\n"

    response += "**Document Readiness:**\n"
    for code in DOCUMENT_REQUIREMENTS:
        readiness = calculate_document_readiness(code, facts)
        if code in completed:
            status = "✅ Done"
        elif readiness['is_ready']:
            status = "🟢 Ready!"
        else:
            status = f"⏳ {readiness['required_percentage']:.0f}%"
        response += f"• {DOCUMENT_REQUIREMENTS[code]['name']}: {status}\n"

    response += "\nWould you like to work on any of these?"
    state['response'] = response
    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'showed_progress'}
    return state


def handle_general_chat(state: ConversationState, action: Dict) -> ConversationState:
    """Entry point for general_chat via router. Activates sticky mode after response."""
    extracted = extract_facts_opportunistically(state, state['user_message'])

    system_prompt = """The user said something that isn't directly about documents.

1. Be friendly and acknowledge briefly (1 sentence)
2. If business info was extracted, mention it naturally
3. Gently steer toward document creation
4. Keep it SHORT — 2-3 sentences max

Respond with JSON: {"response": "your brief response"}"""

    result = invoke_bedrock_json(system_prompt, json.dumps({
        "user_message": state['user_message'],
        "facts_extracted": extracted,
        "business_profile": build_business_profile_summary(state['facts']),
        "conversation_context": format_conversation_for_llm(state['conversation_history'], limit=3)
    }, default=str))

    if result and result.get('response'):
        state['response'] = result['response']
    else:
        state['response'] = "Thanks for sharing! I'm here to help with your business documents. What would you like to work on?"

    # Activate sticky general_chat mode
    state['layer_context'] = {
        'last_action': 'general_chat',
        'sticky_general_chat': True,
        'general_chat_turn_count': 1
    }
    return state


def handle_general_chat_sticky(state: ConversationState) -> ConversationState:
    """Sticky general chat: same behaviour as discovery (11 rules). Used when user exited
    questioning or after at least one document generated. Gets completed_documents and
    optional interrupted_document; post-generation is primary focus."""
    lc = state.get('layer_context', {})
    facts = state['facts']
    facts_count = len([f for f in facts.values() if f.get('value')])
    completed = state.get('completed_documents', [])
    interrupted_doc = state.get('interrupted_document')
    business_profile = build_business_profile_summary(facts)
    total_user_messages = len([t for t in state.get('conversation_history', []) if t.get('role') == 'user'])

    # Document list (same as discovery): CODE, name, short description
    document_list = []
    for code, req in DOCUMENT_REQUIREMENTS.items():
        desc = DOCUMENT_DESCRIPTIONS.get(code, {})
        short_desc = desc.get('short', req.get('name', code))
        document_list.append(f"- {code}: {req.get('name', code)} — {short_desc}")
    document_list_str = "\n".join(document_list)

    # Missing facts for extraction (same as discovery)
    all_fact_ids = set()
    for req in DOCUMENT_REQUIREMENTS.values():
        all_fact_ids.update(req.get('required_facts', []) + req.get('optional_facts', []))
    missing_facts = {fid: FACT_UNIVERSE.get(fid, fid) for fid in all_fact_ids
                     if fid not in facts or not facts[fid].get('value')}
    missing_facts_str = json.dumps(missing_facts) if missing_facts else "{}"

    # Post-generation context
    is_post_gen = state.get('active_layer') == LAYER_POST_GENERATION
    generated_doc = lc.get('generated_doc_name') or lc.get('generated_doc') or None
    interrupted_doc_name = get_document_display_name(interrupted_doc) if interrupted_doc else None

    system_prompt = """You are Cammi's document creation module — same role as in discovery. You suggest, answer inquiries, handle rejects, list documents, and guide users. You have access to completed_documents and optional interrupted_document context.

""" + SCHEDULER_KNOWLEDGE + """

CURRENT STATE:
- Facts collected: {facts_count}
- Business profile: {business_profile}
- Completed documents (already generated): {completed_docs}
- Interrupted document (user left this doc to chat; mention lightly if relevant): {interrupted_doc}
- Post-generation: we just generated a document: {is_post_gen}. If true, celebrate and suggest creating another document as the primary next step.
- Conversation messages so far: {msg_count}

ALL AVAILABLE DOCUMENTS:
{document_list}

RECENT CONVERSATION:
{conversation_history}

EXTRACTABLE FACTS (if user shares business info):
{missing_facts}

YOUR BEHAVIOUR (same 11 rules as discovery):

1. SELECTING A DOCUMENT: If the user commits to creating a specific document (e.g. "let's do ICP", "I want GTM"), set next_action="select_document" and document to the CODE. Your response should confirm the choice enthusiastically.

2. ASKING ABOUT A DOCUMENT: If the user wants to learn about a document before committing, explain it using the document list. Then softly ask if they want to create it. Stay (next_action="stay").

3. SHARING BUSINESS INFO: If the user describes their business, extract facts into extracted_facts. Acknowledge and recommend 1-2 documents that fit. Do NOT extract from questions, hypotheticals, or examples.

4. ASKING FOR RECOMMENDATIONS: If the user asks "where should I start?", "what do you recommend?" — recommend 1-2 documents with brief reasoning. Stay.

5. REJECTING A SUGGESTION: If the user says no to your recommendation, acknowledge and suggest different documents. Stay.

6. ASKING TO SEE ALL DOCUMENTS: Include the document list in your response in a clean format. Stay.

7. POST-GENERATION: If we just completed a document, proactively celebrate and suggest next steps to create a NEW document. IMPORTANT: The user can already view the completed document in their dashboard. DO NOT offer to review, discuss, summarize, or output the generated document in the chat. Provide ONLY options to create the next logical document or ask what new strategy they'd like to work on.

8. GENERAL/OFF-TOPIC: Be friendly and brief, then gently steer back. Stay.

9. DONE: If the user says goodbye, farewell, done — set next_action="done".

10. VIEWING COLLECTED INFO: If the user wants to see what information has been collected ("show me what you have", "my business info", "view my info"), set next_action="show_facts". Do NOT display facts yourself.

11. EDITING COLLECTED INFO: If the user wants to edit or change collected information ("I want to change something", "update my info"), set next_action="edit_fact". Do NOT handle the edit yourself.

12. CAMPAIGNS / SCHEDULING / CONTENT DISTRIBUTION: If the user's intent is about EXECUTING, DISTRIBUTING, or SCHEDULING content rather than creating a document, set next_action="redirect_to_scheduler". This includes but is not limited to:
   - Creating, running, managing, launching, pausing, or tracking campaigns
   - Scheduling, publishing, or planning posts (LinkedIn or social media generally)
   - Content calendar management (viewing, planning, organising a posting timeline)
   - LinkedIn-specific actions (connecting LinkedIn, posting on LinkedIn, LinkedIn outreach)
   - Content promotion, distribution, audience engagement via posts
   - References to the Scheduler tool, Quick Post, or calendar
   - Indirect phrasings like "how do I get my content out there?", "I need to start posting", "can you help me publish?", "plan my posts for next week"
   Acknowledge their interest and let them know this is handled by CAMMI's Scheduler module. Do NOT try to handle campaigns yourself.
   EXCEPTION: If they want to create a DOCUMENT that discusses social/content strategy theoretically, that is document work — do NOT redirect.

IMPORTANT: Same conversational tone as discovery. When the user mentions a document by code or name, map to the correct CODE. For fact extraction, ONLY from first-person statements about THEIR business.

Respond with valid JSON only:
{{"response": "your conversational message", "next_action": "stay" | "select_document" | "show_facts" | "edit_fact" | "redirect_to_scheduler" | "done", "document": "CODE or null", "extracted_facts": {{"fact.id": "value"}} }}"""

    formatted_prompt = system_prompt.format(
        facts_count=facts_count,
        business_profile=business_profile if business_profile else 'No business information collected yet',
        completed_docs=", ".join(completed) if completed else "none",
        interrupted_doc=interrupted_doc_name or "none",
        is_post_gen=is_post_gen,
        msg_count=total_user_messages,
        document_list=document_list_str,
        conversation_history=format_conversation_for_llm(state.get('conversation_history', []), limit=6),
        missing_facts=missing_facts_str,
    )

    result = invoke_bedrock_json(
        formatted_prompt,
        json.dumps({"user_message": state['user_message']}, default=str)
    )
    
    if not result or not result.get('response'):
        print("⚠️ Sticky general_chat LLM returned empty, using fallback")
        state['response'] = "I'm here to help! Tell me more about your business, or let me know if you'd like to create a document."
        state['layer_context'] = {'last_action': 'general_chat', 'sticky_general_chat': True}
        state['last_agent'] = 'general_chat_sticky'
        return state

    response_text = result.get('response', '')
    next_action = result.get('next_action', 'stay')
    document = result.get('document')
    extracted_facts = result.get('extracted_facts', {}) or {}

    print(f"🔄 STICKY GENERAL CHAT: next_action={next_action}, doc={document}, facts={len(extracted_facts)}")

    # Save extracted facts (same as discovery)
    if extracted_facts:
        facts_to_save = {}
        for k, v in extracted_facts.items():
            if v and str(v).strip() and k in FACT_UNIVERSE:
                facts_to_save[k] = str(v).strip()
        if facts_to_save:
            saved_count = save_multiple_facts(state['project_id'], facts_to_save, 'chat')
            print(f"💾 Saved {saved_count} facts from sticky general_chat: {list(facts_to_save.keys())}")
            for fid, value in facts_to_save.items():
                state['facts'][fid] = {'value': value, 'source': 'chat', 'updated_at': datetime.utcnow().isoformat()}

    # Route using same next_action and handlers as discovery
    if next_action == 'select_document' and document and document in DOCUMENT_REQUIREMENTS:
        state['response'] = response_text
        state['last_agent'] = 'general_chat_sticky'
        return handle_select_document(state, {'document': document})

    if next_action == 'show_facts':
        state['last_agent'] = 'general_chat_sticky'
        return handle_show_facts(state, {})

    if next_action == 'edit_fact':
        state['last_agent'] = 'general_chat_sticky'
        return handle_edit_fact(state, {})

    if next_action == 'done':
        state['response'] = response_text
        state['last_agent'] = 'general_chat_sticky'
        return handle_done(state, {})

    if next_action == 'redirect_to_scheduler':
        state['last_agent'] = 'general_chat_sticky'
        return handle_redirect_to_scheduler(state, {})

    # stay or unknown: remain in sticky general chat
    state['response'] = response_text
    state['active_layer'] = state.get('active_layer', LAYER_DISCOVERY)
    state['layer_context'] = {
        'last_action': 'general_chat',
        'sticky_general_chat': True,
        'generated_doc': lc.get('generated_doc'),
        'generated_doc_name': lc.get('generated_doc_name'),
    }
    state['last_agent'] = 'general_chat_sticky'
    return state


def handle_questioning_sticky(state: ConversationState) -> ConversationState:
    """Sticky questioning: one LLM call handles extract OR route OR general. Bypasses router for latency."""
    current_q_id = state.get('current_question_id')
    doc_code = state.get('active_document')
    lc = state.get('layer_context', {})

    if not doc_code or not current_q_id or current_q_id not in GLOBAL_HARVESTERS:
        # Fallback: no active question, go through normal flow
        return handle_start_questioning(state, {})

    harvester = GLOBAL_HARVESTERS[current_q_id]
    primary_facts = harvester['primary_facts']
    secondary_facts = harvester['secondary_facts']
    fact_descriptions = {fid: FACT_UNIVERSE.get(fid, fid) for fid in primary_facts + secondary_facts}
    q_text = harvester['question']

    # ── SHORT-RESPONSE INTERCEPT FOR FOLLOW-UPS ─────────────
    # During a follow-up, short non-answers like "no", "yes", "idk" can't be
    # extracted as facts.  Instead of sending them through the LLM (which
    # mis-classifies them as intent="general" and resets the follow-up state),
    # jump directly to the infer-and-advance path — zero LLM calls.
    is_followup = lc.get('is_followup', False)
    if is_followup:
        msg_clean = state['user_message'].strip().lower().rstrip('.!,')
        _followup_non_answers = {
            'no', 'nah', 'nope', 'n', 'none', 'nothing', 'n/a', 'na',
            'idk', "i don't know", 'dont know', "don't know", 'not sure',
            'no idea', "i don't have that", 'no info',
            'yes', 'yeah', 'yep', 'yea', 'y', 'sure', 'ok', 'okay',
        }
        if msg_clean in _followup_non_answers:
            print(f"⚡ FOLLOWUP SHORT-RESPONSE INTERCEPT: '{msg_clean}' → infer and advance")
            followup_missing = lc.get('missing_facts', [])
            still_missing = [f for f in followup_missing
                             if f not in state['facts'] or not state['facts'][f].get('value')]
            if still_missing:
                inferred = _infer_missing_facts(state, current_q_id, still_missing)
                print(f"🧠 Inferred {inferred} facts for {current_q_id}")
            # Fall through to advancement
            if current_q_id not in state['asked_questions']:
                state['asked_questions'].append(current_q_id)
            if current_q_id in state.get('pending_questions', []):
                state['pending_questions'].remove(current_q_id)
            state['pending_questions'] = determine_pending_questions(
                doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
            )
            readiness = calculate_document_readiness(doc_code, state['facts'])
            if readiness['is_ready']:
                doc_name = get_document_display_name(doc_code)
                state['response'] = f"No problem! I have all the required info for your **{doc_name}**! 🎉\n\nWould you like me to generate it now?"
                state['active_layer'] = LAYER_GENERATION
                state['current_question_id'] = None
                state['layer_context'] = {'last_action': 'ready_to_generate'}
            elif state['pending_questions']:
                next_q_id = state['pending_questions'][0]
                next_q_text = _get_question_text(next_q_id, state['facts'])
                attempts = state.get('question_attempts', {})
                if attempts.get(next_q_id, 0) == 0:
                    attempts[next_q_id] = 1
                state['question_attempts'] = attempts
                state['current_question_id'] = next_q_id
                state['response'] = f"No problem! Next question:\n\n**{next_q_text}**"
                state['active_layer'] = LAYER_QUESTIONING
                state['layer_context'] = {'last_action': 'asked_question', 'question_id': next_q_id}
            else:
                doc_name = get_document_display_name(doc_code)
                state['response'] = (
                    f"No problem! That's all my questions. Your **{doc_name}** is {readiness['required_percentage']:.0f}% complete.\n\n"
                    f"I can generate it now — missing parts will be inferred. Ready?"
                )
                state['current_question_id'] = None
                state['active_layer'] = LAYER_GENERATION
                state['layer_context'] = {'last_action': 'ready_to_generate'}
            state['last_agent'] = 'questioning_sticky'
            return state
    # ── END SHORT-RESPONSE INTERCEPT ─────────────────────────

    # Build document list: CODE → Name (so LLM can map user mentions like "gtm" or "market research" to codes)
    document_list = [
        f"{code}: {DOCUMENT_REQUIREMENTS[code]['name']}"
        for code in DOCUMENT_REQUIREMENTS
    ]
    document_list_str = "\n".join(document_list)

    # Conversation history - last 10 messages for context
    conversation_history = format_conversation_for_llm(state.get('conversation_history', []), limit=10)

    # Build affirmation context (if we offered A/B/C)
    affirm_context = {}
    if lc.get('last_action') == 'offered_suggestions':
        affirm_context = {
            "option_a": lc.get('option_a', ''),
            "option_b": lc.get('option_b', ''),
            "option_c": lc.get('option_c', ''),
            "suggested_answer": lc.get('suggested_answer', ''),
        }

    doc_name = get_document_display_name(doc_code)
    readiness = calculate_document_readiness(doc_code, state['facts'])

    system_prompt = """You are a friendly, sharp business advisor having a real conversation — not running a questionnaire. You're helping the user build a {current_document} by learning about their business one topic at a time. Your job: understand what they just said, extract business facts, and react like a real person who's genuinely interested in their business.

CONTEXT:
- Document: {current_document} ({current_document_code}) — {readiness_pct}% complete
- Current topic: {current_question}
- Facts to extract: {fact_descriptions}
- What we already know: {existing_facts}
- Progress: {questions_asked} questions covered, {questions_remaining} to go

ALL AVAILABLE DOCUMENTS:
{document_list}

RECENT CONVERSATION:
{conversation_history}

{affirmation_section}

YOUR TASK:
Read the user's message in light of the conversation. Determine their intent:

1. ANSWER: They're sharing business info that responds to the current topic.
   - Extract facts: set has_business_info=true, fill extracted_facts with values and confidence 0.0-1.0.
   - Generate an "acknowledgment" (1-2 sentences max) that reacts to what they SPECIFICALLY said. Reference their actual words, ideas, or decisions — show you were listening and that you care about their business.
     GOOD: "A subscription model for small studios — that's a smart angle." / "So the real bottleneck is getting past the IT gatekeepers, interesting." / "Premium pricing with proof to back it up — strong position."
     BAD: "Got it, thanks!" / "Great answer!" / "Thank you for sharing." (these are generic and robotic — NEVER use these)
   - The acknowledgment should feel like a real person reacting: validate their thinking, show curiosity, connect dots to what you already know, or highlight what makes their answer interesting.
   - If nearing the end ({questions_remaining} <= 2), you can naturally weave in "Almost there!" or "Just a couple more."
   - Do NOT ask the next question in the acknowledgment — the system appends it automatically.

2. ROUTE: If they want to do something else:
   - Skip this question → intent="skip"
   - Get help or suggestions → intent="help"
   - View collected facts → intent="show_facts"
   - Generate the document → intent="generate"
   - Switch to a different document OR make a different document OR change document (e.g. "let's go with GTM", "switch to ICP", "do market research", "I want to make another document", "let's do something else", "switch", "change document") → intent="switch", set "document" to the CODE from the list above if they named one, otherwise set document=null
   - Campaigns / scheduling / LinkedIn posting / content calendar / content distribution / publishing posts / content promotion / any intent about EXECUTING or DISTRIBUTING content rather than creating a document → intent="redirect_to_scheduler". The Scheduler is a separate CAMMI module — this chatbot cannot handle campaigns. This covers direct requests ("run a campaign", "schedule a post") AND indirect ones ("how do I get my content out there?", "I need to start posting", "plan my posts for next week").
   - Done/farewell → intent="done"
   - Exit to general chat (user wants to stop answering for now, take a break, chat, come back later, "let's chat", "I'll come back", "not now") → intent="exit_to_general_chat". Provide a short, warm handoff in "response" (e.g. "No problem — we can pick up {doc_name} whenever you're ready. What would you like to do in the meantime?").

3. NEEDS CLARIFICATION: If the user message is ambiguous between generating the document now vs. exiting to chat (e.g. "I'm done", "that's it", "finish up", "done with this") — use intent="needs_clarification" and set "clarification_response" to ONE short disambiguating question in the same turn (e.g. "Want to generate what we have so far, or take a break and chat?"). Do not assume generate or exit.

4. GENERAL: If they said something off-topic, casual, or that doesn't fit above:
- Provide a brief, friendly reply in "brief_response" (1 sentence) and gently redirect back.
- IMPORTANT: Do NOT restate or repeat the current question text inside brief_response (the system will re-ask it separately).
- Set intent="general". Do NOT route to general chat; handle it here.

5. AFFIRM: If we offered A/B/C options and they are affirming without typing their own answer (yes, sounds good, go with B) → intent="affirm_suggestion", picked_option="a"|"b"|"c"|"suggested". If they typed their own answer, use intent="answer".

When the user mentions a document by name, code, or phrase (e.g. "gtm", "go to market", "market research", "ICP"), use the document list to map to the correct CODE. "Let's go with GTM" = switch to GTM.

Respond with valid JSON only:
{{"has_business_info": true/false, "extracted_facts": {{"fact.id": "value"}}, "confidence": {{"fact.id": 0.0-1.0}}, "acknowledgment": "1-2 sentence natural reaction to their answer (only when intent is answer/affirm_suggestion, otherwise empty string)", "intent": "answer"|"skip"|"help"|"show_facts"|"generate"|"switch"|"done"|"exit_to_general_chat"|"needs_clarification"|"general"|"affirm_suggestion", "document": "CODE or null", "brief_response": "short reply for general intent", "response": "handoff message for exit_to_general_chat only", "clarification_response": "one short disambiguating question for needs_clarification only", "picked_option": "a"|"b"|"c"|"suggested" or null}}"""

    affirmation_section = ""
    if affirm_context:
        affirmation_section = """OPTIONS WE JUST OFFERED (user may be affirming one):
- option_a: "{}"
- option_b: "{}"
- option_c: "{}"
- suggested_answer: "{}"
If user affirms without typing their own answer, use intent="affirm_suggestion". If they type their own answer, use intent="answer".""".format(
            affirm_context.get('option_a', ''),
            affirm_context.get('option_b', ''),
            affirm_context.get('option_c', ''),
            affirm_context.get('suggested_answer', ''),
        )

    questions_asked_count = len(state.get('asked_questions', []))
    questions_remaining_count = len(state.get('pending_questions', []))

    formatted_prompt = system_prompt.format(
        doc_name=doc_name,
        current_document=doc_name,
        current_document_code=doc_code,
        readiness_pct=readiness['required_percentage'],
        current_question=q_text,
        fact_descriptions=json.dumps(fact_descriptions, indent=2),
        existing_facts=json.dumps({k: v['value'] for k, v in state['facts'].items() if v.get('value')}, indent=2),
        document_list=document_list_str,
        conversation_history=conversation_history if conversation_history else "(no prior messages)",
        affirmation_section=affirmation_section,
        questions_asked=questions_asked_count,
        questions_remaining=questions_remaining_count,
    )

    context = {
        "user_message": state['user_message'],
    }

    try:
        result = invoke_bedrock_json(formatted_prompt, json.dumps(context, default=str))
    except Exception as e:
        print(f"❌ Questioning sticky LLM error: {e}")
        return handle_process_answer(state, {})  # Fallback to normal flow

    if not result:
        return handle_process_answer(state, {})

    intent = result.get('intent', 'answer')
    has_business_info = result.get('has_business_info', False)
    extracted = result.get('extracted_facts', {}) or {}
    confidence = result.get('confidence', {}) or {}
    document = result.get('document')
    brief_response = result.get('brief_response', '')
    picked_option = result.get('picked_option')
    acknowledgment = result.get('acknowledgment', '').strip()

    print(f"🔄 QUESTIONING STICKY: intent={intent}, has_business_info={has_business_info}, extracted={len(extracted)}, ack={acknowledgment[:60] if acknowledgment else 'none'}")

    # Route to specific handlers
    if intent == 'affirm_suggestion' and lc.get('last_action') == 'offered_suggestions':
        option_map = {'a': lc.get('option_a'), 'b': lc.get('option_b'), 'c': lc.get('option_c'), 'suggested': lc.get('suggested_answer')}
        picked_value = option_map.get(picked_option) if picked_option else lc.get('suggested_answer')
        if picked_value:
            state = _handle_abc_pick(state, picked_value)
            state['last_agent'] = 'questioning_sticky'
            return state

    if intent == 'skip':
        state = handle_skip_question(state, {})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'help':
        state = handle_help_question(state, {})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'show_facts':
        state = handle_show_facts(state, {'document': doc_code})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'generate':
        state = handle_generate_document(state, {'document': doc_code or document})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'switch':
        state = handle_switch_document(state, {'document': document})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'done':
        state = handle_done(state, {})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'redirect_to_scheduler':
        state = handle_redirect_to_scheduler(state, {})
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'exit_to_general_chat':
        handoff = result.get('response') or (
            f"No problem — we can pick up **{doc_name}** whenever you're ready. What would you like to do in the meantime?"
        )
        state['response'] = handoff.strip()
        state['active_document'] = None
        state['interrupted_document'] = doc_code
        state['current_question_id'] = None
        state['pending_questions'] = []
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'exited_to_general_chat', 'sticky_general_chat': True}
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'needs_clarification':
        clarification = result.get('clarification_response') or (
            "Want to generate what we have so far, or take a break and chat?"
        )
        state['response'] = clarification.strip()
        state['active_layer'] = LAYER_QUESTIONING
        state['layer_context'] = {'last_action': 'asked_question', 'question_id': current_q_id}
        state['last_agent'] = 'questioning_sticky'
        return state

    if intent == 'general':
        # Avoid repeating the question if the model already restated it in brief_response.
        resp_lower = (brief_response or "").lower()
        q_lower = (q_text or "").lower()
        should_reask = True
        if q_lower and resp_lower and q_lower in resp_lower:
            should_reask = False

        state['response'] = (brief_response + "\n\n" if brief_response else "")
        if should_reask:
            state['response'] += f"**{q_text}**"
        else:
            state['response'] = state['response'].strip()  # keep it clean if we didn't append
        state['active_layer'] = LAYER_QUESTIONING
        # Preserve follow-up context so the infer-and-advance path stays reachable
        new_lc = {'last_action': 'asked_question', 'question_id': current_q_id}
        if lc.get('is_followup'):
            new_lc['is_followup'] = True
            new_lc['missing_facts'] = lc.get('missing_facts', [])
        state['layer_context'] = new_lc
        state['last_agent'] = 'questioning_sticky'
        return state

    # intent == 'answer' or has_business_info: save facts and advance
    facts_to_save = {}
    for fid, value in extracted.items():
        if value and str(value).strip() and fid in FACT_UNIVERSE:
            fact_conf = confidence.get(fid, 0.8)
            if fact_conf >= 0.6:
                facts_to_save[fid] = str(value).strip()

    extracted_count = 0
    if facts_to_save:
        extracted_count = save_multiple_facts(state['project_id'], facts_to_save, 'chat')
        for fid, value in facts_to_save.items():
            state['facts'][fid] = {'value': value, 'source': 'chat', 'updated_at': datetime.utcnow().isoformat()}

    # ── PARTIAL ANSWER + FOLLOW-UP + INFERENCE LOGIC ─────────
    is_followup = lc.get('is_followup', False)

    # Zero extraction — different handling for first attempt vs follow-up
    if extracted_count == 0 and not is_followup:
        # Normal zero-extraction guard (first attempt)
        attempts = state.get('question_attempts', {})
        current_attempts = attempts.get(current_q_id, 1)
        if current_attempts >= 2:
            missing_descs = [FACT_UNIVERSE.get(f, f) for f in harvester['primary_facts']
                             if f not in state['facts'] or not state['facts'][f].get('value')]
            fact_desc = ', '.join(missing_descs) if missing_descs else 'this information'
            doc_name = get_document_display_name(doc_code)
            state['response'] = (
                f"I wasn't able to capture specific details from that. I was asking about: **{fact_desc}**.\n\n"
                f"No worries — say **skip** to move on, **suggest something** for help, or **generate** to create your {doc_name} now."
            )
        else:
            state['response'] = (
                f"Hmm, I couldn't quite pull specifics from that. Could you try answering more directly?\n\n"
                f"**{q_text}**\n\nOr say **skip** or **suggest something** if you need help!"
            )
        state['active_layer'] = LAYER_QUESTIONING
        state['layer_context'] = {'last_action': 'asked_question', 'question_id': current_q_id}
        attempts[current_q_id] = current_attempts + 1
        state['question_attempts'] = attempts
        state['last_agent'] = 'questioning_sticky'
        return state

    if extracted_count == 0 and is_followup:
        # Follow-up attempt with zero extraction — silently infer the missing facts
        followup_missing = lc.get('missing_facts', [])
        still_missing = [f for f in followup_missing
                         if f not in state['facts'] or not state['facts'][f].get('value')]
        if still_missing:
            inferred = _infer_missing_facts(state, current_q_id, still_missing)
            print(f"🧠 Follow-up zero-extraction: inferred {inferred} facts for {current_q_id}")
        # Fall through to advancement below

    # Check for partial answer (some facts extracted but primary facts still missing)
    if extracted_count > 0:
        missing_primary = [f for f in harvester['primary_facts']
                           if f not in state['facts'] or not state['facts'][f].get('value')]

        if missing_primary and not is_followup:
            # First partial answer — rephrase for missing facts only
            followup_q = _rephrase_for_missing_facts(current_q_id, missing_primary, state['facts'])
            ack = acknowledgment if acknowledgment else "Got it!"
            state['response'] = f"{ack} {followup_q}"
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {
                'last_action': 'asked_followup',
                'question_id': current_q_id,
                'is_followup': True,
                'missing_facts': missing_primary
            }
            state['last_agent'] = 'questioning_sticky'
            return state

        elif missing_primary and is_followup:
            # Second attempt still missing — silently infer the rest
            still_missing = [f for f in missing_primary
                             if f not in state['facts'] or not state['facts'][f].get('value')]
            if still_missing:
                inferred = _infer_missing_facts(state, current_q_id, still_missing)
                print(f"🧠 Follow-up partial: inferred {inferred} remaining facts for {current_q_id}")
            # Fall through to advancement below

    # ── ADVANCE: mark question asked, move to next or ready_to_generate ──
    if current_q_id not in state['asked_questions']:
        state['asked_questions'].append(current_q_id)
    if current_q_id in state.get('pending_questions', []):
        state['pending_questions'].remove(current_q_id)

    state['pending_questions'] = determine_pending_questions(
        doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
    )
    readiness = calculate_document_readiness(doc_code, state['facts'])

    # Build conversational transition from LLM acknowledgment (falls back to simple ack)
    if acknowledgment:
        ack_line = acknowledgment
    elif extracted_count > 0:
        ack_line = "Got it!"
    else:
        ack_line = ""

    if readiness['is_ready']:
        doc_name = get_document_display_name(doc_code)
        ready_msg = f"I have all the required info for your **{doc_name}**! 🎉\n\nWould you like me to generate it now?"
        state['response'] = f"{ack_line}\n\n{ready_msg}" if ack_line else ready_msg
        state['active_layer'] = LAYER_GENERATION
        state['current_question_id'] = None
        state['layer_context'] = {'last_action': 'ready_to_generate'}
    elif state['pending_questions']:
        next_q_id = state['pending_questions'][0]
        next_q_text = _get_question_text(next_q_id, state['facts'])
        attempts = state.get('question_attempts', {})
        if attempts.get(next_q_id, 0) == 0:
            attempts[next_q_id] = 1
        state['question_attempts'] = attempts
        state['current_question_id'] = next_q_id
        state['response'] = f"{ack_line}\n\n**{next_q_text}**" if ack_line else f"**{next_q_text}**"
        state['active_layer'] = LAYER_QUESTIONING
        state['layer_context'] = {'last_action': 'asked_question', 'question_id': next_q_id}
    else:
        doc_name = get_document_display_name(doc_code)
        all_done_msg = (
            f"That's all my questions! Your **{doc_name}** is {readiness['required_percentage']:.0f}% complete.\n\n"
            f"I can generate it now — missing parts will be inferred. Ready?"
        )
        state['response'] = f"{ack_line}\n\n{all_done_msg}" if ack_line else all_done_msg
        state['current_question_id'] = None
        state['active_layer'] = LAYER_GENERATION
        state['layer_context'] = {'last_action': 'ready_to_generate'}

    state['last_agent'] = 'questioning_sticky'
    return state


def handle_start_questioning(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = state.get('active_document')

    # If action specifies a document, switch to it
    if action.get('document') and action['document'] in DOCUMENT_REQUIREMENTS:
        doc_code = action['document']
        state['active_document'] = doc_code

    if not doc_code:
        state['response'] = f"Let's pick a document first!\n\n{build_document_list_text(state)}"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'asked_which_document'}
        return state

    # Refresh pending questions
    state['pending_questions'] = determine_pending_questions(
        doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
    )

    if state['pending_questions']:
        q_id = state['pending_questions'][0]
        q_text = _get_question_text(q_id, state['facts'])
        state['current_question_id'] = q_id

        attempts = state.get('question_attempts', {})
        if attempts.get(q_id, 0) == 0:
            attempts[q_id] = 1
        state['question_attempts'] = attempts

        state['response'] = f"**{q_text}**"
        state['active_layer'] = LAYER_QUESTIONING
        state['layer_context'] = {'last_action': 'asked_question', 'question_id': q_id}
    else:
        readiness = calculate_document_readiness(doc_code, state['facts'])
        doc_name = get_document_display_name(doc_code)
        if readiness['is_ready']:
            state['response'] = f"I have all the info for your **{doc_name}**! 🎉 Ready to generate?"
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
        else:
            state['response'] = (
                f"I've asked all my questions. Your **{doc_name}** is {readiness['required_percentage']:.0f}% complete.\n\n"
                f"I can generate it now (missing parts will be inferred). Want to proceed?"
            )
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}

    return state


def handle_process_answer(state: ConversationState, action: Dict) -> ConversationState:
    current_q_id = state.get('current_question_id')
    doc_code = state.get('active_document')
    lc = state.get('layer_context', {})

    if not current_q_id or current_q_id not in GLOBAL_HARVESTERS:
        # No active question — try opportunistic extraction and move on
        extracted = extract_facts_opportunistically(state, state['user_message'])
        if doc_code:
            return handle_start_questioning(state, action)
        state['response'] = "Thanks for that info! What would you like to work on?"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'general_chat'}
        return state

    # ─────────────────────────────────────────────────────────
    # SUGGESTION AFFIRMATION CHECK
    # If last action was offered_suggestions and user is affirming
    # (e.g., "yes", "go with that", "save it"), use the suggested_answer.
    # Note: bare A/B/C picks are intercepted in dispatch() before reaching here.
    # This handles natural language affirmations like "sounds good", "save that", etc.
    # ─────────────────────────────────────────────────────────
    last_action = lc.get('last_action', '')
    used_suggestion = False

    if last_action == 'offered_suggestions' and lc.get('suggested_answer'):
        msg_lower = state['user_message'].lower().strip()
        stored_suggestion = lc.get('suggested_answer', '')

        affirmation_phrases = [
            'yes', 'yeah', 'yep', 'yup', 'sure', 'okay', 'ok', 'sounds good',
            'go with that', 'save that', 'save it', 'save this', 'use that',
            'use this', 'that works', 'works for me', 'go ahead', 'perfect',
            'great', 'good', 'fine', 'your answer', 'your suggestion',
            'that sounds good', 'that sounds right', 'sounds right',
            'sounds correct', 'go with your', 'use your', 'save your',
            'i like that', 'i like it', 'let\'s go with that', 'let\'s use that',
            'absolutely', 'definitely', 'please', 'do it', 'approved',
            'i agree', 'agreed', 'correct', 'right', 'exactly'
        ]

        option_picking_phrases = [
            'go with a', 'go with b', 'go with c', 'go with option a',
            'go with option b', 'go with option c', 'option a', 'option b',
            'option c', 'the first', 'the second', 'the third',
            'first one', 'second one', 'third one', 'number 1', 'number 2',
            'number 3', 'choice a', 'choice b', 'choice c'
        ]

        is_affirming = any(phrase in msg_lower for phrase in affirmation_phrases)
        is_picking_option = any(phrase in msg_lower for phrase in option_picking_phrases)

        if is_affirming or is_picking_option:
            # Resolve which option they picked (for multi-word picks like "go with B")
            picked_value = stored_suggestion  # default to suggested_answer
            for letter, key in [('a', 'option_a'), ('b', 'option_b'), ('c', 'option_c')]:
                if letter in msg_lower and lc.get(key):
                    picked_value = lc[key]
                    break

            return _handle_abc_pick(state, picked_value)
    # ─────────────────────────────────────────────────────────
    # END SUGGESTION AFFIRMATION CHECK
    # ─────────────────────────────────────────────────────────

    # Normal extraction from user's actual message
    extracted_count = _extract_facts_from_answer(state)

    # ─────────────────────────────────────────────────────────
    # PARTIAL ANSWER + FOLLOW-UP + INFERENCE (process_answer path)
    # ─────────────────────────────────────────────────────────
    is_followup = lc.get('is_followup', False)
    harvester = GLOBAL_HARVESTERS[current_q_id]

    # Zero extraction — different handling for first attempt vs follow-up
    if extracted_count == 0 and not used_suggestion and not is_followup:
        msg_lower = state['user_message'].lower().strip()

        # FIRST: Check if this looks like a help request that was misrouted
        help_phrases = [
            'suggest', 'suggestion', 'help me', 'give me ideas', 'give me options',
            'what should i say', 'give examples', 'can you help', 'help me answer',
            'what are some', 'give me some', "i'm not sure", 'uncertain'
        ]
        if any(phrase in msg_lower for phrase in help_phrases):
            print(f"🔀 Zero-extraction guard detected help request, rerouting to help handler")
            return handle_help_question(state, {})

        # Check if the user mentioned a document code or name
        doc_codes_lower = {c.lower(): c for c in DOCUMENT_REQUIREMENTS}
        doc_names_lower = {v['name'].lower(): k for k, v in DOCUMENT_DESCRIPTIONS.items()}

        mentioned_doc = None
        for code_lower, code in doc_codes_lower.items():
            pattern = r'\b' + re.escape(code_lower) + r'\b'
            if re.search(pattern, msg_lower):
                mentioned_doc = code
                break
        if not mentioned_doc:
            for name_lower, code in doc_names_lower.items():
                if name_lower in msg_lower:
                    mentioned_doc = code
                    break

        if mentioned_doc:
            if mentioned_doc == doc_code:
                q_text = GLOBAL_HARVESTERS[current_q_id]['question']
                readiness = calculate_document_readiness(doc_code, state['facts'])
                doc_name = get_document_display_name(doc_code)
                state['response'] = (
                    f"We're already working on **{doc_name}** — "
                    f"{readiness['required_percentage']:.0f}% complete! "
                    f"Here's the current question:\n\n**{q_text}**\n\n"
                    f"You can answer this, say **skip** to move on, or say **show facts** to see what I've collected."
                )
                state['active_layer'] = LAYER_QUESTIONING
                state['layer_context'] = {'last_action': 'asked_question', 'question_id': current_q_id}
                return state
            else:
                return handle_inquire_document(state, {'document': mentioned_doc})

        attempts = state.get('question_attempts', {})
        current_attempts = attempts.get(current_q_id, 1)

        if current_attempts >= 2:
            q_text = GLOBAL_HARVESTERS[current_q_id]['question']
            missing_descs = [FACT_UNIVERSE.get(f, f) for f in harvester['primary_facts']
                             if f not in state['facts'] or not state['facts'][f].get('value')]
            fact_desc = ', '.join(missing_descs) if missing_descs else 'this information'
            doc_name = get_document_display_name(doc_code) if doc_code else 'your document'
            readiness = calculate_document_readiness(doc_code, state['facts']) if doc_code else None
            readiness_pct = f"{readiness['required_percentage']:.0f}%" if readiness else "unknown"

            state['response'] = (
                f"I wasn't able to capture specific details from that. "
                f"I was asking about: **{fact_desc}**.\n\n"
                f"No worries — here's what you can do:\n"
                f"- **Answer with specifics** (names, numbers, descriptions) and I'll capture it\n"
                f"- Say **skip** to move to the next question\n"
                f"- Say **I don't know** or **suggest something** and I'll help you think through it\n"
                f"- Say **generate** to create your {doc_name} now ({readiness_pct} complete — missing info will be inferred)\n"
                f"- Or say the name of a different document to switch"
            )
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {'last_action': 'asked_question', 'question_id': current_q_id}
            attempts[current_q_id] = current_attempts + 1
            state['question_attempts'] = attempts
            return state
        else:
            q_text = GLOBAL_HARVESTERS[current_q_id]['question']
            state['response'] = (
                f"Hmm, I couldn't quite pull specifics from that. "
                f"Could you try answering this one more directly?\n\n"
                f"**{q_text}**\n\n"
                f"Or say **skip** if you'd rather move on, or **suggest something** if you need help!"
            )
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {'last_action': 'asked_question', 'question_id': current_q_id}
            attempts[current_q_id] = current_attempts + 1
            state['question_attempts'] = attempts
            return state

    if extracted_count == 0 and is_followup:
        # Follow-up attempt with zero extraction — silently infer the missing facts
        followup_missing = lc.get('missing_facts', [])
        still_missing = [f for f in followup_missing
                         if f not in state['facts'] or not state['facts'][f].get('value')]
        if still_missing:
            inferred = _infer_missing_facts(state, current_q_id, still_missing)
            print(f"🧠 process_answer follow-up zero-extraction: inferred {inferred} facts")
        # Fall through to advancement

    # Check for partial answer (some facts extracted but primary facts still missing)
    if extracted_count > 0:
        missing_primary = [f for f in harvester['primary_facts']
                           if f not in state['facts'] or not state['facts'][f].get('value')]

        if missing_primary and not is_followup:
            # First partial answer — rephrase for missing facts only
            followup_q = _rephrase_for_missing_facts(current_q_id, missing_primary, state['facts'])
            state['response'] = f"Got it, thanks! {followup_q}"
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {
                'last_action': 'asked_followup',
                'question_id': current_q_id,
                'is_followup': True,
                'missing_facts': missing_primary
            }
            return state

        elif missing_primary and is_followup:
            # Second attempt still missing — silently infer the rest
            still_missing = [f for f in missing_primary
                             if f not in state['facts'] or not state['facts'][f].get('value')]
            if still_missing:
                inferred = _infer_missing_facts(state, current_q_id, still_missing)
                print(f"🧠 process_answer follow-up partial: inferred {inferred} remaining facts")
            # Fall through to advancement

    # ─────────────────────────────────────────────────────────
    # ADVANCE: mark question asked, move to next or ready_to_generate
    # ─────────────────────────────────────────────────────────
    if current_q_id not in state['asked_questions']:
        state['asked_questions'].append(current_q_id)
    if current_q_id in state.get('pending_questions', []):
        state['pending_questions'].remove(current_q_id)

    if doc_code:
        state['pending_questions'] = determine_pending_questions(
            doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
        )
        readiness = calculate_document_readiness(doc_code, state['facts'])

        if readiness['is_ready']:
            doc_name = get_document_display_name(doc_code)
            ack = "Got it, thanks! " if extracted_count > 0 else ""
            state['response'] = (
                f"{ack}Wonderful! I have all the required info for your **{doc_name}**! 🎉\n\n"
                f"Would you like me to generate it now?"
            )
            state['active_layer'] = LAYER_GENERATION
            state['current_question_id'] = None
            state['layer_context'] = {'last_action': 'ready_to_generate'}
            return state

        if state['pending_questions']:
            next_q_id = state['pending_questions'][0]
            next_q_text = GLOBAL_HARVESTERS[next_q_id]['question']

            attempts = state.get('question_attempts', {})
            if attempts.get(next_q_id, 0) == 0:
                attempts[next_q_id] = 1
            state['question_attempts'] = attempts
            state['current_question_id'] = next_q_id

            ack = "Got it, thanks! " if extracted_count > 0 else "Thanks! "
            state['response'] = f"{ack}Next question:\n\n**{next_q_text}**"
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {'last_action': 'asked_question', 'question_id': next_q_id}
        else:
            doc_name = get_document_display_name(doc_code)
            state['response'] = (
                f"That's all my questions! Your **{doc_name}** is {readiness['required_percentage']:.0f}% complete.\n\n"
                f"I can generate it now — missing parts will be inferred. Ready?"
            )
            state['current_question_id'] = None
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
    else:
        state['response'] = "Thanks! What would you like to do next?"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'general_chat'}

    return state


def handle_help_question(state: ConversationState, action: Dict) -> ConversationState:
    """User needs help with a question - explain simply + give A/B/C suggestions."""
    return _provide_question_help(state, action)


def _provide_question_help(state: ConversationState, action: Dict) -> ConversationState:
    """Unified helper for both explain_question and idk_question - provides simple explanation + suggestions."""
    current_q_id = state.get('current_question_id')
    attempt = state.get('question_attempts', {}).get(current_q_id, 1) if current_q_id else 1

    if not current_q_id or current_q_id not in GLOBAL_HARVESTERS:
        state['response'] = "I'm not asking a specific question right now. What can I help with?"
        state['layer_context'] = {'last_action': 'general_chat'}
        return state

    harvester = GLOBAL_HARVESTERS[current_q_id]

    if attempt >= 3:
        state['response'] = "No worries at all! Let's skip this one and move on."
        return handle_skip_question(state, action)

    # Gather ALL known facts for rich context
    known_facts_summary = []
    for fid, fdata in state['facts'].items():
        if fdata.get('value'):
            fact_name = FACT_UNIVERSE.get(fid, fid)
            known_facts_summary.append(f"- {fact_name}: {fdata['value']}")
    known_facts_str = "\n".join(known_facts_summary) if known_facts_summary else "No facts collected yet."

    system_prompt = """Help the user answer a business question. Return ONLY valid JSON with these 5 short fields. Keep each field brief so JSON stays valid.

CRITICAL DISTINCTION:
- simple_explanation: Explain what the QUESTION MEANS in general. What concept are we asking about? Teach it like to a 10-year-old. Use plain words, no jargon. Do NOT give their specific answer. Example: for "key messages" you'd explain "The goal is to pinpoint the most important ideas you want customers to remember about your brand. These are the core takeaways they should have after interacting with your company."
- option_a, option_b, option_c: NOW get specific. Given THEIR business from KNOWN FACTS, what could their answer BE? 3 concrete options personalized to their business. Each one short sentence.
- suggested_answer: One clean sentence combining the most likely option. For extraction when user picks A/B/C.

KNOWN FACTS ABOUT THEIR BUSINESS:
{known_facts}

QUESTION: {question}

Return JSON with exactly these keys (no extra text, no markdown):
{{"simple_explanation": "string", "option_a": "string", "option_b": "string", "option_c": "string", "suggested_answer": "string"}}"""

    try:
        result = invoke_bedrock_json(
            system_prompt.format(known_facts=known_facts_str, question=harvester['question']),
            json.dumps({
                "question": harvester['question'],
                "fact_descriptions": [FACT_UNIVERSE.get(f, f) for f in harvester['primary_facts']],
                "document": get_document_display_name(state['active_document']) if state.get('active_document') else 'document'
            }, default=str)
        )
        print(f"🔍 QUESTION_HELP LLM RESULT: {json.dumps(result, default=str)[:500] if result else 'EMPTY'}")
    except Exception as e:
        print(f"❌ QUESTION_HELP LLM ERROR: {str(e)}")
        result = None

    # New format: short fields (avoids JSON parse failure from long escaped strings)
    if result and result.get('simple_explanation'):
        simple = result.get('simple_explanation', '').strip()
        opt_a = result.get('option_a', '').strip()
        opt_b = result.get('option_b', '').strip()
        opt_c = result.get('option_c', '').strip()
        suggested_answer = result.get('suggested_answer', '').strip() or opt_a

        state['response'] = (
            f"{simple}\n\n"
            f"For your business, it could be:\n\n"
            f"**A)** {opt_a}\n\n"
            f"**B)** {opt_b}\n\n"
            f"**C)** {opt_c}\n\n"
            f"I hope that helps! You can pick one of my suggestions (just say A, B, or C), or type your own version."
        )
        state['layer_context'] = {
            'last_action': 'offered_suggestions',
            'question_id': current_q_id,
            'option_a': opt_a,
            'option_b': opt_b,
            'option_c': opt_c,
            'suggested_answer': suggested_answer
        }
    elif result and result.get('response'):
        # Backward compat: LLM returned old format
        state['response'] = result['response']
        suggested_answer = result.get('suggested_answer', '').strip() or result['response']
        state['layer_context'] = {
            'last_action': 'offered_suggestions',
            'question_id': current_q_id,
            'option_a': '',
            'option_b': '',
            'option_c': '',
            'suggested_answer': suggested_answer
        }
    else:
        # Enhanced fallback
        fact_desc = FACT_UNIVERSE.get(harvester['primary_facts'][0], 'this information')
        business_name = state['facts'].get('business.name', {}).get('value', 'your business')
        business_desc = state['facts'].get('business.description_short', {}).get('value', '')
        
        context_hint = f"For {business_name}"
        if business_desc:
            context_hint += f" ({business_desc})"
        
        state['response'] = (
            f"Let me help! I'm asking about: **{fact_desc}**\n\n"
            f"{context_hint}, think about what applies. Even a rough answer helps! 😊\n\n"
            f"**{harvester['question']}**\n\n"
            f"You can answer, say **skip** to move on, or I can try explaining differently!"
        )
        state['layer_context'] = {
            'last_action': 'offered_suggestions',
            'question_id': current_q_id,
            'option_a': '',
            'option_b': '',
            'option_c': '',
            'suggested_answer': ''
        }

    # Increment attempt
    attempts = state.get('question_attempts', {})
    attempts[current_q_id] = attempt + 1
    state['question_attempts'] = attempts

    state['active_layer'] = LAYER_QUESTIONING
    return state


def handle_skip_question(state: ConversationState, action: Dict) -> ConversationState:
    current_q_id = state.get('current_question_id')

    if current_q_id:
        if current_q_id not in state['asked_questions']:
            state['asked_questions'].append(current_q_id)
        if current_q_id in state.get('pending_questions', []):
            state['pending_questions'].remove(current_q_id)
        if current_q_id not in state.get('skipped_questions', []):
            state['skipped_questions'].append(current_q_id)

    # Move to next question
    return handle_start_questioning(state, action)


# handle_idk_question and handle_explain_question are now merged into handle_help_question above


def handle_show_facts(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = action.get('document') or state.get('active_document')
    facts = state['facts']
    facts_count = len([f for f in facts.values() if f.get('value')])

    # Zero-facts case — friendly message instead of empty display
    if facts_count == 0:
        state['response'] = (
            "I don't have any information about your business yet! "
            "You can tell me a bit about your business and I'll note it down, "
            "or pick a document to work on and I'll ask you some questions to get started."
        )
        state['layer_context'] = {
            'last_action': 'showed_facts_empty',
            'pre_edit_layer': state.get('active_layer', LAYER_DISCOVERY),
            'pre_edit_document': doc_code
        }
        return state

    if doc_code:
        formatted, fact_mapping = format_facts_for_display(facts, doc_code)
        doc_name = get_document_display_name(doc_code)
        state['response'] = (
            f"Here's what I know for your **{doc_name}**:\n{formatted}\n\n"
            f"Would you like to edit anything, or shall we continue?"
        )
    else:
        formatted, fact_mapping = format_facts_for_display(facts)
        state['response'] = (
            f"Here's everything I know:\n{formatted}\n\n"
            f"Would you like to edit anything?"
        )

    # Store pre-edit context so we can return to it
    state['active_layer'] = LAYER_REVIEW
    state['layer_context'] = {
        'last_action': 'showed_facts',
        'fact_mapping': fact_mapping,
        'pre_edit_layer': state.get('active_layer', LAYER_DISCOVERY),
        'pre_edit_document': doc_code
    }
    return state


def handle_edit_fact(state: ConversationState, action: Dict) -> ConversationState:
    facts = state['facts']
    lc = state.get('layer_context', {})
    improve_row_map = lc.get('improve_row_map', {}) if isinstance(lc.get('improve_row_map', {}), dict) else {}

    # Preserve pre-edit context if not already set
    if 'pre_edit_layer' not in lc:
        lc['pre_edit_layer'] = state.get('active_layer', LAYER_DISCOVERY)
        lc['pre_edit_document'] = state.get('active_document')
        lc['pre_edit_question_id'] = state.get('current_question_id')

    # If we already identified a fact and are waiting for new value
    if lc.get('last_action') == 'asked_new_value' and lc.get('editing_fact'):
        editing_fact = lc['editing_fact']
        new_value = state['user_message'].strip()
        
        # IMMEDIATE SAVE (no confirmation)
        save_fact(state['project_id'], editing_fact, new_value, 'chat')
        state['facts'][editing_fact] = {
            'value': new_value,
            'source': 'chat',
            'updated_at': datetime.utcnow().isoformat()
        }
        
        fact_name = FACT_UNIVERSE.get(editing_fact, editing_fact)
        improve_active = bool(lc.get('improve_active'))
        doc_code_local = state.get('active_document')
        readiness = calculate_document_readiness(doc_code_local, state['facts']) if doc_code_local else None

        if improve_active and readiness and readiness['is_ready']:
            doc_name = get_document_display_name(doc_code_local)
            state['response'] = (
                f"✅ Your **{fact_name}** has been updated to: {new_value}\n\n"
                f"Your **{doc_name}** now has all required information. Would you like me to generate it now?"
            )
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate', 'improve_active': True}
            return state

        state['response'] = (
            f"✅ Your **{fact_name}** has been updated to: {new_value}\n\n"
            f"Would you like to edit anything else?"
        )
        state['active_layer'] = LAYER_REVIEW
        state['layer_context'] = {
            'last_action': 'edit_completed',
            'last_edited_fact': editing_fact,
            'improve_active': improve_active,
            'improve_row_map': improve_row_map,
            'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
            'pre_edit_document': lc.get('pre_edit_document'),
            'pre_edit_question_id': lc.get('pre_edit_question_id')
        }
        return state

    # Build fact mapping for identification
    doc_code = state.get('active_document')
    if doc_code:
        _, fact_mapping = format_facts_for_display(facts, doc_code)
    else:
        _, fact_mapping = format_facts_for_display(facts)

    fact_list_parts = []
    for num, fid in fact_mapping.items():
        value = facts.get(fid, {}).get('value', 'not set')
        name = FACT_UNIVERSE.get(fid, fid)
        fact_list_parts.append(f"#{num} ({fid}): {name} = {value}")

    if not fact_list_parts:
        state['response'] = "No facts collected yet. Tell me about your business first!"
        state['layer_context'] = {'last_action': 'no_facts'}
        return state

    edit_target = action.get('edit_target', '')
    edit_value = action.get('edit_value', '')

    improve_map_text = ""
    if improve_row_map:
        improve_map_text = (
            "\nIMPROVE ROW NUMBER MAP (user may refer to these numbers):\n"
            + "\n".join([f"- {num} -> {fid}" for num, fid in improve_row_map.items()])
        )

    multi_edit_prompt = f"""Extract ALL fact edits from the user message.

USER MESSAGE: "{state['user_message']}"

AVAILABLE FACTS (format: #NUMBER (fact_id): description = current_value):
{chr(10).join(fact_list_parts)}
{improve_map_text}

RULES:
1. Return every clear edit instruction in the message.
2. Map each edit to a fact_id from the available list.
3. Include only edits where both fact_id and new_value are clear.
4. If no clear edits, return an empty list.
5. If user says "change 1"/"change 2" and improve row map exists, use that map first.

Respond ONLY as JSON:
{{"edits": [{{"fact_id": "exact.fact.id", "new_value": "new value"}}]}}"""

    multi_result = invoke_bedrock_json(multi_edit_prompt, json.dumps({"user_message": state['user_message']}, default=str))
    candidate_edits = (multi_result or {}).get('edits', [])
    if isinstance(candidate_edits, list):
        normalized_multi = []
        for edit in candidate_edits:
            if not isinstance(edit, dict):
                continue
            fid = edit.get('fact_id')
            val = edit.get('new_value')
            if not fid or not val:
                continue
            if fid not in FACT_UNIVERSE:
                cleaned = str(fid).strip().lstrip('#').strip()
                if cleaned in improve_row_map:
                    fid = improve_row_map[cleaned]
                if cleaned in fact_mapping:
                    fid = fact_mapping[cleaned]
            if fid in FACT_UNIVERSE and str(val).strip():
                normalized_multi.append((fid, str(val).strip()))

        if len(normalized_multi) >= 2:
            dedup = {}
            for fid, val in normalized_multi:
                dedup[fid] = val

            updated_lines = []
            for fid, val in dedup.items():
                save_fact(state['project_id'], fid, val, 'chat')
                state['facts'][fid] = {
                    'value': val,
                    'source': 'chat',
                    'updated_at': datetime.utcnow().isoformat()
                }
                updated_lines.append(f"- **{FACT_UNIVERSE.get(fid, fid)}**: {val}")

            improve_active = bool(lc.get('improve_active'))
            if doc_code:
                readiness = calculate_document_readiness(doc_code, state['facts'])
                if improve_active and readiness['is_ready']:
                    doc_name = get_document_display_name(doc_code)
                    state['response'] = (
                        "✅ I updated those fields:\n\n"
                        + "\n".join(updated_lines)
                        + f"\n\nYour **{doc_name}** now has all required information. Would you like me to generate it now?"
                    )
                    state['active_layer'] = LAYER_GENERATION
                    state['layer_context'] = {'last_action': 'ready_to_generate', 'improve_active': True}
                    return state

            state['response'] = (
                "✅ I updated those fields:\n\n"
                + "\n".join(updated_lines)
                + "\n\nWould you like to edit anything else?"
            )
            state['active_layer'] = LAYER_REVIEW
            state['layer_context'] = {
                'last_action': 'edit_completed',
                'last_edited_fact': list(dedup.keys())[-1],
                'improve_active': improve_active,
                'improve_row_map': improve_row_map,
                'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
                'pre_edit_document': lc.get('pre_edit_document'),
                'pre_edit_question_id': lc.get('pre_edit_question_id')
            }
            return state

    system_prompt = f"""You must identify which fact the user wants to edit and what the new value should be.

USER MESSAGE: "{state['user_message']}"
EDIT HINT target: "{edit_target}"
EDIT HINT value: "{edit_value}"

HERE ARE ALL THE AVAILABLE FACTS (format: #NUMBER (fact_id): description = current_value):
{chr(10).join(fact_list_parts)}
{improve_map_text}

STRICT RULES:
1. The user may refer to a fact by its NUMBER (e.g., "change 4", "edit #2", "update number 1") or by its NAME/DESCRIPTION (e.g., "change the value proposition", "edit customer problems").
2. Match the user's reference to the correct fact from the list above.
3. The "fact_id" in your response MUST be the EXACT fact_id from the parentheses in the list above — for example "product.value_proposition_short" or "customer.problems". NEVER return just a number like "4" or "#4".
4. If the user provides a new value (e.g., "change 4 to XYZ"), extract "XYZ" as the new_value.
5. If the user only identifies a fact but doesn't give a new value (e.g., "edit 4" or "change the value proposition"), set new_value to null.
6. If you cannot determine which fact the user means, set needs_clarification to true.
7. If user uses improve-table numbering (e.g., "change 1 to ...") and a row map is provided, map using that row number.

EXAMPLES:
- User says "change 4 to From idea to impact" → {{"fact_id": "product.value_proposition_short", "new_value": "From idea to impact", "needs_clarification": false}}
- User says "edit number 2" → {{"fact_id": "customer.primary_customer", "new_value": null, "needs_clarification": false}}
- User says "update the business description to We build AI tools" → {{"fact_id": "business.description_short", "new_value": "We build AI tools", "needs_clarification": false}}
- User says "change something" → {{"fact_id": null, "new_value": null, "needs_clarification": true}}

Respond with ONLY this JSON format:
{{"fact_id": "exact.fact.id.from.list", "new_value": "new value or null", "needs_clarification": false}}"""

    result = invoke_bedrock_json(system_prompt, json.dumps({"user_message": state['user_message']}, default=str))

    if result:
        fact_id = result.get('fact_id')
        new_value = result.get('new_value')

        # SAFETY NET: If LLM returned a number instead of a fact_id, resolve it
        if fact_id and fact_id not in FACT_UNIVERSE:
            # Strip common prefixes like "#", "number", etc.
            cleaned = str(fact_id).strip().lstrip('#').strip()
            if cleaned in improve_row_map:
                fact_id = improve_row_map[cleaned]
            if cleaned in fact_mapping:
                fact_id = fact_mapping[cleaned]

        if fact_id and new_value and fact_id in FACT_UNIVERSE:
            # IMMEDIATE SAVE (no confirmation)
            save_fact(state['project_id'], fact_id, new_value, 'chat')
            state['facts'][fact_id] = {
                'value': new_value,
                'source': 'chat',
                'updated_at': datetime.utcnow().isoformat()
            }
            
            fact_name = FACT_UNIVERSE.get(fact_id, fact_id)
            improve_active = bool(lc.get('improve_active'))
            readiness = calculate_document_readiness(doc_code, state['facts']) if doc_code else None

            if improve_active and readiness and readiness['is_ready']:
                doc_name = get_document_display_name(doc_code)
                state['response'] = (
                    f"✅ Your **{fact_name}** has been updated to: {new_value}\n\n"
                    f"Your **{doc_name}** now has all required information. Would you like me to generate it now?"
                )
                state['active_layer'] = LAYER_GENERATION
                state['layer_context'] = {'last_action': 'ready_to_generate', 'improve_active': True}
                return state

            state['response'] = (
                f"✅ Your **{fact_name}** has been updated to: {new_value}\n\n"
                f"Would you like to edit anything else?"
            )
            state['active_layer'] = LAYER_REVIEW
            state['layer_context'] = {
                'last_action': 'edit_completed',
                'last_edited_fact': fact_id,
                'improve_active': improve_active,
                'improve_row_map': improve_row_map,
                'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
                'pre_edit_document': lc.get('pre_edit_document'),
                'pre_edit_question_id': lc.get('pre_edit_question_id')
            }
            return state

        elif fact_id and not new_value and fact_id in FACT_UNIVERSE:
            fact_name = FACT_UNIVERSE.get(fact_id, fact_id)
            current = facts.get(fact_id, {}).get('value', 'not set')
            state['response'] = f"Current value for **{fact_name}**: {current}\n\nWhat would you like to change it to?"
            state['active_layer'] = LAYER_REVIEW
            state['layer_context'] = {
                'last_action': 'asked_new_value',
                'editing_fact': fact_id,
                'improve_active': bool(lc.get('improve_active')),
                'improve_row_map': improve_row_map,
                'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
                'pre_edit_document': lc.get('pre_edit_document'),
                'pre_edit_question_id': lc.get('pre_edit_question_id')
            }
            return state

    # Couldn't identify — show facts for them to pick
    if doc_code:
        formatted, fact_mapping = format_facts_for_display(facts, doc_code)
        state['response'] = f"Which fact would you like to edit?\n{formatted}\n\nJust say the number or name!"
    else:
        formatted, fact_mapping = format_facts_for_display(facts)
        state['response'] = f"Which fact would you like to edit?\n{formatted}\n\nJust say the number or name!"

    state['active_layer'] = LAYER_REVIEW
    state['layer_context'] = {
        'last_action': 'showed_facts_for_editing',
        'fact_mapping': fact_mapping,
        'improve_active': bool(lc.get('improve_active')),
        'improve_row_map': improve_row_map
    }
    return state


def handle_confirm_edit(state: ConversationState, action: Dict) -> ConversationState:
    # This handler is now mostly obsolete since we save immediately,
    # but keeping it for backward compatibility in case it's called
    lc = state.get('layer_context', {})
    pending_fact = lc.get('pending_edit_fact')
    pending_value = lc.get('pending_edit_value')

    if pending_fact and pending_value:
        save_fact(state['project_id'], pending_fact, pending_value, 'chat')
        state['facts'][pending_fact] = {
            'value': pending_value,
            'source': 'chat',
            'updated_at': datetime.utcnow().isoformat()
        }
        fact_name = FACT_UNIVERSE.get(pending_fact, pending_fact)
        state['response'] = (
            f"✅ Your **{fact_name}** has been updated to: {pending_value}\n\n"
            f"Would you like to edit anything else?"
        )
        state['active_layer'] = LAYER_REVIEW
        state['layer_context'] = {
            'last_action': 'edit_completed',
            'last_edited_fact': pending_fact,
            'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
            'pre_edit_document': lc.get('pre_edit_document'),
            'pre_edit_question_id': lc.get('pre_edit_question_id')
        }
    else:
        state['response'] = "I'm not sure what to confirm. What would you like to edit?"
        state['active_layer'] = LAYER_REVIEW
        state['layer_context'] = {'last_action': 'asked_what_to_edit'}

    return state


def handle_cancel_edit(state: ConversationState, action: Dict) -> ConversationState:
    lc = state.get('layer_context', {})
    state['response'] = "No problem, edit cancelled. Would you like to edit anything else?"
    state['active_layer'] = LAYER_REVIEW
    state['layer_context'] = {
        'last_action': 'edit_completed',
        'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
        'pre_edit_document': lc.get('pre_edit_document'),
        'pre_edit_question_id': lc.get('pre_edit_question_id')
    }
    return state


def handle_correct_edit(state: ConversationState, action: Dict) -> ConversationState:
    """User wants to correct the edit they just made (e.g., 'wait no, change it to X')."""
    lc = state.get('layer_context', {})
    last_edited_fact = lc.get('last_edited_fact')
    
    if not last_edited_fact:
        # No recent edit to correct
        state['response'] = "I'm not sure which edit you want to correct. Which fact would you like to change?"
        state['active_layer'] = LAYER_REVIEW
        state['layer_context'] = {'last_action': 'asked_what_to_edit'}
        return state
    
    # Extract new value from user message
    new_value = action.get('edit_value', '').strip()
    
    if not new_value:
        # Try to extract from message
        msg = state['user_message'].strip()
        # Look for patterns like "wait, change it to X" or "actually X"
        patterns = [
            r'change it to\s+(.+)',
            r'make it\s+(.+)',
            r'actually\s+(.+)',
            r'no,?\s+(.+)',
            r'wait,?\s+(.+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, msg, re.IGNORECASE)
            if match:
                new_value = match.group(1).strip()
                break
    
    if not new_value:
        # Ask for clarification
        fact_name = FACT_UNIVERSE.get(last_edited_fact, last_edited_fact)
        current = state['facts'].get(last_edited_fact, {}).get('value', 'not set')
        state['response'] = f"Current value for **{fact_name}**: {current}\n\nWhat would you like to change it to?"
        state['active_layer'] = LAYER_REVIEW
        state['layer_context'] = {
            'last_action': 'asked_new_value',
            'editing_fact': last_edited_fact,
            'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
            'pre_edit_document': lc.get('pre_edit_document'),
            'pre_edit_question_id': lc.get('pre_edit_question_id')
        }
        return state
    
    # IMMEDIATE SAVE
    save_fact(state['project_id'], last_edited_fact, new_value, 'chat')
    state['facts'][last_edited_fact] = {
        'value': new_value,
        'source': 'chat',
        'updated_at': datetime.utcnow().isoformat()
    }
    
    fact_name = FACT_UNIVERSE.get(last_edited_fact, last_edited_fact)
    state['response'] = (
        f"✅ Your **{fact_name}** has been updated to: {new_value}\n\n"
        f"Would you like to edit anything else?"
    )
    state['active_layer'] = LAYER_REVIEW
    state['layer_context'] = {
        'last_action': 'edit_completed',
        'last_edited_fact': last_edited_fact,
        'pre_edit_layer': lc.get('pre_edit_layer', LAYER_DISCOVERY),
        'pre_edit_document': lc.get('pre_edit_document'),
        'pre_edit_question_id': lc.get('pre_edit_question_id')
    }
    return state


def handle_improve_document_quality(state: ConversationState, action: Dict) -> ConversationState:
    """Infer missing required facts, auto-save them, and present editable suggestions."""
    doc_code = state.get('active_document')
    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        state['response'] = (
            "I can improve document quality once a document is selected. "
            "Please choose a document first, then click Improve Now."
        )
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'improve_requires_document'}
        return state

    doc_name = get_document_display_name(doc_code)
    required_facts = get_required_facts_for_document(doc_code)

    existing = state.get('facts', {})
    missing_required = [fid for fid in required_facts if fid not in existing or not existing[fid].get('value')]

    if not missing_required:
        state['response'] = (
            f"Your **{doc_name}** already has all required information collected. "
            f"Would you like me to generate it now?"
        )
        state['active_layer'] = LAYER_GENERATION
        state['layer_context'] = {'last_action': 'ready_to_generate'}
        return state

    known_facts = {k: v['value'] for k, v in existing.items() if v.get('value')}
    missing_descriptions = {fid: FACT_UNIVERSE.get(fid, fid) for fid in missing_required}

    infer_prompt = """You are inferring missing business facts for a document-generation assistant.

DOCUMENT:
- code: {doc_code}
- name: {doc_name}

RULES:
1. Infer ONLY for the missing facts listed.
2. Do NOT overwrite already known facts.
3. Use all known business context to maximize plausibility and specificity.
4. Keep each inferred value concise and professional.
5. If a fact truly cannot be inferred, set it to null.

Return ONLY valid JSON:
{{"inferred_facts": {{"fact.id": "value or null"}}}}"""

    result = invoke_bedrock_json(
        infer_prompt.format(doc_code=doc_code, doc_name=doc_name),
        json.dumps({
            "known_facts": known_facts,
            "missing_facts": missing_descriptions,
            "required_facts": required_facts
        }, default=str)
    )

    inferred_raw = (result or {}).get('inferred_facts', {}) or {}

    inferred_facts = {}
    for fid, value in inferred_raw.items():
        if fid in missing_required and value and str(value).strip():
            inferred_facts[fid] = str(value).strip()

    if not inferred_facts:
        state['response'] = (
            f"I tried to improve your **{doc_name}** quality using everything available, "
            f"but I still need a bit more direct input from you. "
            f"You can continue answering questions, or say **show facts** to edit manually."
        )
        state['layer_context'] = {'last_action': 'improve_no_inference'}
        return state

    confirmed_required_count = len([fid for fid in required_facts if fid in existing and existing[fid].get('value')])
    suggested_required_count = len(inferred_facts)

    table_rows = []
    improve_row_map = {}
    row_idx = 1
    for fid in required_facts:
        if fid in inferred_facts:
            field_name = FACT_UNIVERSE.get(fid, fid)
            display_value = _truncate_preview_value(inferred_facts[fid])
            table_rows.append({
                'number': row_idx,
                'field': field_name,
                'suggested_value': display_value
            })
            improve_row_map[str(row_idx)] = fid
            row_idx += 1
    suggested_dict_text = json.dumps({
        'suggested_fields': table_rows
    }, indent=2)

    intro_line = (
        f"Using what you've already shared, I've taken care of the remaining details needed to create the **{doc_name}**."
    )

    state['response'] = (
        f"{intro_line}\n\n"
        f"I’ve created a table summarizing the information I worked out for your document to improve quality.\n\n"
        f"**Suggested Required Fields**\n"
        f"{suggested_dict_text}\n\n"
        f"I have already applied these suggested values to your document draft. "
        f"If anything needs changes, type your edits. "
        f"You can reference either field names or row numbers (for example: change 1 to ... or change description to ...).\n\n"
        f"If this looks good, say **generate** and I will create your **{doc_name}** now."
    )

    saved_count = save_multiple_facts(state['project_id'], inferred_facts, source='chat')
    print(f"💾 Improve flow auto-saved {saved_count} inferred facts for {doc_code}")
    for fid, value in inferred_facts.items():
        state['facts'][fid] = {
            'value': value,
            'source': 'chat',
            'updated_at': datetime.utcnow().isoformat()
        }

    prior_layer = state.get('active_layer', LAYER_DISCOVERY)
    state['layer_context'] = {
        'last_action': 'improve_applied',
        'improve_active': True,
        'improve_doc': doc_code,
        'improve_row_map': improve_row_map,
        'pre_infer_layer': prior_layer,
        'pre_infer_question_id': state.get('current_question_id')
    }
    state['active_layer'] = LAYER_REVIEW
    return state


def handle_confirm_inferred_facts_and_generate(state: ConversationState, action: Dict) -> ConversationState:
    """Persist pending inferred facts and generate the current document."""
    lc = state.get('layer_context', {})
    doc_code = lc.get('pending_inferred_doc') or state.get('active_document')
    pending = lc.get('pending_inferred_facts', {}) or {}

    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        state['response'] = "I couldn't determine which document to generate. Please select a document first."
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'improve_confirm_missing_doc'}
        return state

    if pending:
        saved_count = save_multiple_facts(state['project_id'], pending, source='inferred')
        print(f"💾 Saved {saved_count} inferred facts on improve-confirm for {doc_code}")
        for fid, value in pending.items():
            state['facts'][fid] = {
                'value': value,
                'source': 'inferred',
                'updated_at': datetime.utcnow().isoformat()
            }

    state['active_document'] = doc_code
    state['generating_document'] = None
    state['current_question_id'] = None
    return _execute_generation(state)


def handle_decline_inferred_facts(state: ConversationState, action: Dict) -> ConversationState:
    """User rejected suggested inferred facts; keep conversation safe and editable."""
    lc = state.get('layer_context', {})
    doc_code = lc.get('pending_inferred_doc') or state.get('active_document')
    pre_layer = lc.get('pre_infer_layer', LAYER_DISCOVERY)

    state['response'] = (
        "No problem — I will not use those suggestions. "
        "You can continue with your own inputs, or say **show facts** to edit what I have."
    )

    state['active_document'] = doc_code
    state['current_question_id'] = lc.get('pre_infer_question_id')

    if pre_layer == LAYER_QUESTIONING and doc_code:
        state['active_layer'] = LAYER_QUESTIONING
        state['pending_questions'] = determine_pending_questions(
            doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
        )
    else:
        state['active_layer'] = LAYER_REVIEW

    state['layer_context'] = {'last_action': 'declined_inferred_facts'}
    return state


def handle_generate_document(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = action.get('document') or state.get('active_document')

    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        state['response'] = f"Which document would you like to generate?\n\n{build_document_list_text(state)}"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'asked_which_document'}
        return state

    # Force-generate path from questioning: infer missing required + supporting facts
    # for the selected document before generation.
    if state.get('active_layer') == LAYER_QUESTIONING:
        _infer_missing_document_facts_for_force_generate(state, doc_code)

    state['active_document'] = doc_code
    return _execute_generation(state)


def handle_cancel_generation(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = state.get('active_document')
    doc_name = get_document_display_name(doc_code) if doc_code else 'the document'
    state['response'] = f"No problem! We can generate your **{doc_name}** anytime. What would you like to do?"
    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'cancelled_generation'}
    return state

def handle_decline_generation(state: ConversationState, action: Dict) -> ConversationState:
    """User said no to generating — offer clear alternative paths."""
    doc_code = state.get('active_document')
    doc_name = get_document_display_name(doc_code) if doc_code else 'the document'
    facts = state['facts']

    response_parts = [f"No problem! Your **{doc_name}** isn't going anywhere — we can generate it whenever you're ready.\n"]

    options = []

    # Option 1: Review/edit facts
    facts_count = len([f for f in facts.values() if f.get('value')])
    if facts_count > 0:
        options.append("**Review or edit** the information I've collected so far")

    # Option 2: Answer more supporting questions
    if doc_code:
        readiness = calculate_document_readiness(doc_code, facts)
        missing_supporting = readiness.get('missing_supporting', [])
        if missing_supporting:
            options.append(f"**Answer a few more questions** to strengthen the document ({len(missing_supporting)} optional questions remaining)")

    # Option 3: Switch document
    completed = state.get('completed_documents', [])
    available_count = len([c for c in DOCUMENT_REQUIREMENTS if c not in completed and c != doc_code])
    if available_count > 0:
        options.append("**Switch to a different document** entirely")

    if options:
        response_parts.append("Here's what we can do instead:\n")
        for i, opt in enumerate(options, 1):
            response_parts.append(f"{i}. {opt}")
        response_parts.append("\nWhat sounds good?")
    else:
        response_parts.append("What would you like to do instead?")

    state['response'] = "\n".join(response_parts)
    state['active_layer'] = LAYER_REVIEW
    state['layer_context'] = {'last_action': 'declined_generation'}
    return state


def handle_switch_document(state: ConversationState, action: Dict) -> ConversationState:
    doc_code = action.get('document')
    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        doc_code = _extract_document_from_message(state['user_message'])

    # Check layer_context for inquired_doc if we still don't have a doc
    if not doc_code or doc_code not in DOCUMENT_REQUIREMENTS:
        lc = state.get('layer_context', {})
        inquired = lc.get('inquired_doc')
        if inquired and inquired in DOCUMENT_REQUIREMENTS:
            doc_code = inquired

    if doc_code and doc_code in DOCUMENT_REQUIREMENTS:
        # Section eligibility check
        if doc_code in ALIGN_DOCUMENTS:
            eligibility = check_align_eligibility(state['completed_documents'])
            if not eligibility['is_eligible']:
                doc_name = get_document_display_name(doc_code)
                clarify_count = eligibility['clarify_count']
                min_required = eligibility['min_required']
                state['response'] = (
                    f"I'd love to help you with the **{doc_name}**, but you'll need to complete "
                    f"at least {min_required} Clarify documents first. "
                    f"You've completed {clarify_count} so far.\n\n"
                    f"Once you hit {min_required}, you'll unlock the Align section!"
                )
                state['layer_context'] = {'last_action': 'blocked_align_access'}
                state['active_layer'] = LAYER_DISCOVERY
                return state

        # Update current_tab based on target document's section
        new_tab = SECTION_ALIGN if doc_code in ALIGN_DOCUMENTS else SECTION_CLARIFY
        if new_tab != state.get('current_tab'):
            state['current_tab'] = new_tab
            # If switching to Align via specific document, mark intro as shown
            new_align_intro = True if new_tab == SECTION_ALIGN else state.get('align_intro_shown', False)
            state['align_intro_shown'] = new_align_intro
            update_section_state(state['project_id'], new_tab, state['completed_documents'], align_intro_shown=new_align_intro)

        old_name = get_document_display_name(state.get('active_document')) if state.get('active_document') else 'previous document'
        new_name = get_document_display_name(doc_code)

        state['active_document'] = doc_code
        state['current_question_id'] = None
        state['pending_questions'] = determine_pending_questions(
            doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
        )

        readiness = calculate_document_readiness(doc_code, state['facts'])
        state['response'] = f"Switching to **{new_name}**! Your progress on {old_name} is saved.\n\n"

        if readiness['is_ready']:
            state['response'] += "I already have all the info — shall I generate it?"
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
        else:
            state['active_layer'] = LAYER_QUESTIONING
            return handle_start_questioning(state, action)
    else:
        state['response'] = f"Which document would you like to switch to?\n\n{build_document_list_text(state)}"
        state['active_document'] = None
        state['current_question_id'] = None
        state['pending_questions'] = []
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'asked_which_document'}

    return state


def handle_continue(state: ConversationState, action: Dict) -> ConversationState:
    """Resume wherever we left off."""
    lc = state.get('layer_context', {})
    
    # Check if we're coming from an edit flow
    if lc.get('last_action') in ['edit_completed', 'showed_facts']:
        pre_edit_layer = lc.get('pre_edit_layer')
        pre_edit_document = lc.get('pre_edit_document')
        pre_edit_question_id = lc.get('pre_edit_question_id')
        
        # Return to previous context
        if pre_edit_layer == LAYER_QUESTIONING and pre_edit_document:
            state['active_document'] = pre_edit_document
            state['current_question_id'] = pre_edit_question_id
            return handle_start_questioning(state, action)
        elif pre_edit_layer == LAYER_GENERATION and pre_edit_document:
            state['active_document'] = pre_edit_document
            doc_name = get_document_display_name(pre_edit_document)
            state['response'] = f"Great! Your **{doc_name}** is ready. Would you like me to generate it now?"
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
            return state
    
    # Default continue behavior
    doc_code = state.get('active_document')

    if doc_code:
        readiness = calculate_document_readiness(doc_code, state['facts'])
        if readiness['is_ready']:
            doc_name = get_document_display_name(doc_code)
            state['response'] = f"Your **{doc_name}** is ready! Would you like me to generate it?"
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
        else:
            return handle_start_questioning(state, action)
    else:
        state['response'] = f"What would you like to work on?\n\n{build_document_list_text(state)}"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'showed_doc_list'}

    return state


def handle_done(state: ConversationState, action: Dict) -> ConversationState:
    state['response'] = "Thanks for chatting! Your progress is saved — come back anytime. 👋"
    state['active_layer'] = LAYER_DISCOVERY
    state['layer_context'] = {'last_action': 'user_done'}
    return state


def handle_redirect_to_scheduler(state: ConversationState, action: Dict) -> ConversationState:
    """Guide user to the Scheduler module for campaigns, LinkedIn posting, and content scheduling."""
    doc_code = state.get('active_document')
    doc_name = get_document_display_name(doc_code) if doc_code else None

    response_parts = [
        "That's a great idea! 🚀 Campaigns and content scheduling are handled by **CAMMI's Scheduler** module.\n"
    ]

    response_parts.append(
        "Here's what you can do in the Scheduler:\n"
        "- **Quick Post** — Create and schedule a single LinkedIn post instantly\n"
        "- **Create New Campaign** — Import a URL or paste content, and CAMMI builds an SEO-optimized campaign (Awareness, Consideration, or Conversion)\n"
        "- **Use Existing Campaign** — Continue working on a campaign you already started\n"
        "- **View Calendar** — Manage all your scheduled posts in a calendar timeline\n"
    )

    response_parts.append(
        f"👉 **Head there directly:**\n{SCHEDULER_URL}\n\n"
        "Or just click **Scheduler** in the left sidebar in the **Tools** section."
    )

    if doc_name:
        response_parts.append(
            f"\nBy the way, your **{doc_name}** progress is saved here — you can come back anytime to continue!"
        )

    state['response'] = "\n".join(response_parts)
    # Preserve current layer so user can seamlessly return
    state['layer_context'] = {
        'last_action': 'redirected_to_scheduler',
        'pre_redirect_document': doc_code,
    }
    return state


# ============================================================================
# FACT EXTRACTION FROM ANSWER (UNCHANGED LOGIC)
# ============================================================================

def _extract_facts_from_answer(state: ConversationState) -> int:
    current_q_id = state.get('current_question_id')
    if not current_q_id or current_q_id not in GLOBAL_HARVESTERS:
        return 0

    harvester = GLOBAL_HARVESTERS[current_q_id]
    primary_facts = harvester['primary_facts']
    secondary_facts = harvester['secondary_facts']

    fact_descriptions = {}
    for fid in primary_facts + secondary_facts:
        fact_descriptions[fid] = FACT_UNIVERSE.get(fid, fid)

    system_prompt = """Extract business facts from the user's answer.

RULES:
1. ONLY extract what is EXPLICITLY stated
2. Do NOT infer or assume
3. Use user's exact words where possible
4. If something is vague but present, extract anyway
5. If user says "I don't know", extract nothing

Respond with JSON:
{"extracted_facts": {"fact.id": "value or null"}, "confidence": {"fact.id": 0.0 to 1.0}}"""

    context = {
        "question_asked": harvester['question'],
        "user_response": state['user_message'],
        "primary_facts_to_extract": {fid: fact_descriptions[fid] for fid in primary_facts},
        "secondary_facts_to_extract": {fid: fact_descriptions[fid] for fid in secondary_facts},
        "existing_facts": {k: v['value'] for k, v in state['facts'].items() if v.get('value')}
    }

    result = invoke_bedrock_json(system_prompt, json.dumps(context, default=str))

    extracted_count = 0
    if result:
        extracted = result.get('extracted_facts', {})
        confidence = result.get('confidence', {})
        facts_to_save = {}
        for fid, value in extracted.items():
            if value and str(value).strip() and fid in FACT_UNIVERSE:
                fact_conf = confidence.get(fid, 0.8)
                if fact_conf >= 0.6:
                    facts_to_save[fid] = str(value).strip()
        if facts_to_save:
            saved = save_multiple_facts(state['project_id'], facts_to_save, 'chat')
            extracted_count = saved
            for fid, value in facts_to_save.items():
                state['facts'][fid] = {
                    'value': value,
                    'source': 'chat',
                    'updated_at': datetime.utcnow().isoformat()
                }

    return extracted_count


# ============================================================================
# PARTIAL ANSWER HELPERS — rephrase follow-up + silent inference
# ============================================================================

def _get_question_text(q_id: str, facts: Dict[str, Dict[str, Any]]) -> str:
    """Return the question text for a harvester, rephrased if some primary facts are already filled."""
    harvester = GLOBAL_HARVESTERS.get(q_id, {})
    primary_facts = harvester.get('primary_facts', [])
    original_text = harvester.get('question', '')

    # Which primary facts are still missing?
    missing_primary = [f for f in primary_facts
                       if f not in facts or not facts[f].get('value')]

    # All missing → use original question as-is
    if len(missing_primary) == len(primary_facts):
        return original_text

    # All filled → shouldn't happen (harvester wouldn't be selected), but fallback
    if not missing_primary:
        return original_text

    # Some filled, some missing → rephrase to only ask about missing ones
    return _rephrase_for_missing_facts(q_id, missing_primary, facts)


def _rephrase_for_missing_facts(q_id: str, missing_facts: List[str], facts: Dict[str, Dict[str, Any]]) -> str:
    """Generate a natural follow-up question that asks ONLY about the missing facts."""
    harvester = GLOBAL_HARVESTERS.get(q_id, {})
    missing_descriptions = {fid: FACT_UNIVERSE.get(fid, fid) for fid in missing_facts}

    system_prompt = """Generate a brief, natural follow-up question that asks ONLY about the missing information.
The user already partially answered a question. Now ask about ONLY what's still missing.

RULES:
1. Be conversational and warm
2. Do NOT repeat information the user already provided
3. Keep it to 1-2 sentences max
4. Ask specifically about the missing facts listed below
5. Do NOT start with "Great" or "Thanks" — the caller will prepend an acknowledgement

MISSING INFORMATION NEEDED:
{missing_facts}

Respond with JSON: {{"question": "your follow-up question"}}"""

    result = invoke_bedrock_json(
        system_prompt.format(missing_facts=json.dumps(missing_descriptions, indent=2)),
        json.dumps({
            "original_question": harvester.get('question', ''),
            "known_facts": {k: v['value'] for k, v in facts.items() if v.get('value')}
        }, default=str)
    )

    if result and result.get('question'):
        return result['question']

    descs = [FACT_UNIVERSE.get(f, f) for f in missing_facts]
    if len(descs) == 1:
        return f"Could you also tell me about **{descs[0]}**?"
    return f"Could you also tell me about: {', '.join(descs[:-1])} and {descs[-1]}?"


def _infer_missing_facts(state: ConversationState, q_id: str, missing_facts: List[str]) -> int:
    """Silently infer missing facts from the full business profile. Returns count of inferred facts."""
    facts = state['facts']
    known_facts = {k: v['value'] for k, v in facts.items() if v.get('value')}
    missing_descriptions = {fid: FACT_UNIVERSE.get(fid, fid) for fid in missing_facts}
    harvester = GLOBAL_HARVESTERS.get(q_id, {})

    system_prompt = """Based on all the business information provided, infer reasonable values for the missing facts.

KNOWN BUSINESS INFORMATION:
{known_facts}

MISSING FACTS TO INFER:
{missing_facts}

RULES:
1. Make reasonable inferences based on the known information
2. Keep inferred values concise and professional
3. If you truly cannot infer a fact even loosely, set its value to null
4. Use the business context to make educated guesses
5. Be specific — vague guesses are worse than null

Respond with JSON: {{"inferred_facts": {{"fact.id": "inferred value or null"}}}}"""

    result = invoke_bedrock_json(
        system_prompt.format(
            known_facts=json.dumps(known_facts, indent=2),
            missing_facts=json.dumps(missing_descriptions, indent=2)
        ),
        json.dumps({"harvester_question": harvester.get('question', '')}, default=str)
    )

    inferred_count = 0
    if result:
        inferred = result.get('inferred_facts', {})
        for fid, value in inferred.items():
            if value and str(value).strip() and fid in FACT_UNIVERSE:
                save_fact(state['project_id'], fid, str(value).strip(), 'chat')
                state['facts'][fid] = {
                    'value': str(value).strip(),
                    'source': 'chat',
                    'updated_at': datetime.utcnow().isoformat()
                }
                inferred_count += 1
        if inferred_count > 0:
            print(f"🧠 Inferred {inferred_count} facts for {q_id}: {[f for f in inferred if inferred[f]]}")

    return inferred_count


def _infer_missing_document_facts_for_force_generate(state: ConversationState, doc_code: str) -> int:
    """Infer and save missing required + supporting facts for a selected document.

    This is used by force generation from the questioning flow.
    Low-confidence facts are still saved by design.
    """
    all_doc_facts = get_all_facts_for_document(doc_code)
    facts = state.get('facts', {})

    missing_facts = [
        fid for fid in all_doc_facts
        if fid not in facts or not facts[fid].get('value')
    ]

    if not missing_facts:
        return 0

    known_facts = {k: v.get('value') for k, v in facts.items() if v.get('value')}
    missing_descriptions = {fid: FACT_UNIVERSE.get(fid, fid) for fid in missing_facts}

    system_prompt = """Infer missing document facts from known business context.

DOCUMENT CONTEXT:
- code: {doc_code}
- name: {doc_name}

RULES:
1. Infer values for all missing facts listed when reasonably possible.
2. Return confidence for each inferred fact.
3. If a fact truly cannot be inferred, set it to null.
4. Keep outputs concise and business-relevant.

Respond ONLY with valid JSON:
{{
  "inferred_facts": {{"fact.id": "value or null"}},
  "confidence": {{"fact.id": 0.0 to 1.0}}
}}"""

    result = invoke_bedrock_json(
        system_prompt.format(
            doc_code=doc_code,
            doc_name=get_document_display_name(doc_code)
        ),
        json.dumps({
            "known_facts": known_facts,
            "missing_facts": missing_descriptions,
            "required_facts": get_required_facts_for_document(doc_code),
            "supporting_facts": get_supporting_facts_for_document(doc_code)
        }, default=str)
    )

    if not result:
        return 0

    inferred_raw = result.get('inferred_facts', {}) or {}
    confidence_raw = result.get('confidence', {}) or {}

    # Save all non-empty inferred values regardless of confidence threshold.
    facts_to_save = {}
    for fid, value in inferred_raw.items():
        if fid in missing_facts and value and str(value).strip():
            facts_to_save[fid] = str(value).strip()

    if not facts_to_save:
        return 0

    saved_count = save_multiple_facts(state['project_id'], facts_to_save, source='inferred')
    for fid, value in facts_to_save.items():
        state['facts'][fid] = {
            'value': value,
            'source': 'inferred',
            'updated_at': datetime.utcnow().isoformat()
        }

    low_conf_count = sum(
        1 for fid in facts_to_save
        if isinstance(confidence_raw.get(fid), (int, float)) and confidence_raw.get(fid) < 0.6
    )
    print(
        f"🧠 FORCE GENERATE INFERENCE: doc={doc_code}, saved={saved_count}, "
        f"low_conf_saved={low_conf_count}, total_missing={len(missing_facts)}"
    )

    return saved_count


# ============================================================================
# DOCUMENT EXTRACTION HELPER (UNCHANGED)
# ============================================================================

def _extract_document_from_message(user_message: str) -> Optional[str]:
    prompt = """Extract the document code the user is referring to.

DOCUMENTS (code → name; also match common phrases/aliases):
- GTM → Go-to-Market Plan (also: go to market, go-to-market)
- ICP → Ideal Customer Profile (also: ideal customer, ideal customer profile)
- ICP2 → Persona Deep Dive (also: persona, persona deep dive)
- MESSAGING → Messaging Document (also: messaging, messaging doc)
- BRAND → Brand Document (also: brand identity, brand doc)
- MR → Market Research (also: market research, research)
- KMF → Key Messaging Framework (also: key messaging framework, messaging framework)
- SR → Strategy Roadmap (also: strategy roadmap, roadmap)
- SMP → Strategic Marketing Plan (also: strategic marketing plan, marketing plan)
- BS → Brand Strategy (also: brand strategy)

Rules:
- If the message clearly refers to a document name/alias, return its code.
- If it's ambiguous, return null with low confidence.

Return ONLY valid JSON:
{"document_code": "CODE or null", "confidence": 0.0 to 1.0}"""

    result = invoke_bedrock_json(prompt, f'Message: "{user_message}"')
    if result:
        code = result.get('document_code')
        conf = result.get('confidence', 0)
        if code and conf >= 0.6 and code in DOCUMENT_REQUIREMENTS:
            return code
    return None


# ============================================================================
# GENERATION EXECUTION (UNCHANGED LOGIC)
# ============================================================================

def _execute_generation(state: ConversationState) -> ConversationState:
    doc_code = state.get('active_document')
    if not doc_code:
        state['active_layer'] = LAYER_DISCOVERY
        state['response'] = "No document selected. Which would you like to create?"
        state['last_agent'] = 'generation'
        return state

    doc_name = get_document_display_name(doc_code)

    try:
        qa_content = _build_document_qa_content(doc_code, state['facts'])
        doc_code_lower = doc_code.lower()
        s3_key = f"{doc_code_lower}/{state['project_id']}/prompt/businessidea/businessidea/businessidea.txt"

        # s3_client.put_object(
        #     Bucket=S3_BUCKET,
        #     Key=s3_key,
        #     Body=qa_content.encode('utf-8'),
        #     ContentType='text/plain',
        #     Metadata={
        #         'token': state['session_id'],
        #         'project_id': state['project_id'],
        #         'document_type': doc_code_lower
        #     }
        # )
        print(f"PIPELINE INPUT UPLOADED: s3://{S3_BUCKET}/{s3_key}")

        if doc_code not in state['completed_documents']:
            state['completed_documents'].append(doc_code)
            # Sync completed documents with section state table
            update_section_state(state['project_id'], state['current_tab'], state['completed_documents'], align_intro_shown=state.get('align_intro_shown', False))

        next_suggestion = _build_next_doc_suggestion(doc_code, state['completed_documents'])

        state['response'] = (
            f"Excellent! I'm generating your **{doc_name}**! 🚀\n\n"
            f"This takes a few moments. I've gathered all your business information and "
            f"I'm crafting a professional document tailored to your needs.\n\n"
            f"You'll receive your document shortly. ✨"
        )

        state['active_layer'] = LAYER_POST_GENERATION
        state['generating_document'] = doc_code
        state['active_document'] = None
        state['current_question_id'] = None
        state['pending_questions'] = []
        state['layer_context'] = {
            'last_action': 'document_generated',
            'generated_doc': doc_code,
            'generated_doc_name': doc_name,
            'sticky_general_chat': True,
        }

    except Exception as e:
        print(f"Error generating document: {e}")
        import traceback
        traceback.print_exc()
        state['response'] = f"I encountered an issue preparing your **{doc_name}**. Would you like me to try again?"
        state['layer_context'] = {'last_action': 'generation_failed', 'failed_doc': doc_code}

    state['last_agent'] = 'generation'
    return state


def _build_document_qa_content(doc_code: str, facts: Dict[str, Dict[str, Any]]) -> str:
    required = get_required_facts_for_document(doc_code)
    supporting = get_supporting_facts_for_document(doc_code)
    lines = ["=== REQUIRED INFORMATION ===\n"]
    for fid in required:
        desc = FACT_UNIVERSE.get(fid, fid)
        value = facts.get(fid, {}).get('value', 'Not provided')
        lines.append(f"Q: {desc}")
        lines.append(f"A: {value}\n")
    lines.append("\n=== SUPPORTING INFORMATION ===\n")
    for fid in supporting:
        desc = FACT_UNIVERSE.get(fid, fid)
        value = facts.get(fid, {}).get('value', 'Not provided')
        lines.append(f"Q: {desc}")
        lines.append(f"A: {value}\n")
    return "\n".join(lines)


def _build_next_doc_suggestion(doc_code: str, completed: List[str]) -> str:
    # Check if user just became eligible for Align (exactly hit the threshold with this document)
    if doc_code in CLARIFY_DOCUMENTS:
        eligibility = check_align_eligibility(completed)
        # Check if they JUST became eligible (exactly at threshold)
        if eligibility['is_eligible'] and eligibility['clarify_count'] == MIN_CLARIFY_DOCS_FOR_ALIGN:
            return "🎉 **Milestone unlocked!** You've completed 2 Clarify documents — you can now access the Align section and create documents like the Customer Charter or Quarterly Marketing Plan!"

    progression = DOCUMENT_PROGRESSION.get(doc_code, {})
    for next_code in progression.get('natural_next', []):
        if next_code not in completed and next_code in DOCUMENT_DESCRIPTIONS:
            reasoning = progression.get('reasoning', {}).get(next_code, '')
            next_name = DOCUMENT_DESCRIPTIONS[next_code]['name']
            if reasoning:
                return f"💡 **What's next?** A **{next_name}** would be a great next step — {reasoning.lower()}"
            return f"💡 **What's next?** A **{next_name}** would complement this nicely."
    return ""


# ============================================================================
# ACTION DISPATCH TABLE
# ============================================================================

ACTION_HANDLERS = {
    "greet": handle_greet,
    "describe_business": handle_describe_business,
    "select_document": handle_select_document,
    "inquire_document": handle_inquire_document,
    "recommend_documents": handle_recommend_documents,
    "reject_recommendation": handle_reject_recommendation,
    "list_documents": handle_list_documents,
    "show_progress": handle_show_progress,
    "general_chat": handle_general_chat,
    "start_questioning": handle_start_questioning,
    "process_answer": handle_process_answer,
    "help_question": handle_help_question,
    "skip_question": handle_skip_question,
    "show_facts": handle_show_facts,
    "edit_fact": handle_edit_fact,
    "confirm_edit": handle_confirm_edit,
    "cancel_edit": handle_cancel_edit,
    "correct_edit": handle_correct_edit,
    "improve_document_quality": handle_improve_document_quality,
    "confirm_inferred_facts_and_generate": handle_confirm_inferred_facts_and_generate,
    "decline_inferred_facts": handle_decline_inferred_facts,
    "generate_document": handle_generate_document,
    "cancel_generation": handle_cancel_generation,
    "decline_generation": handle_decline_generation,
    "switch_document": handle_switch_document,
    "redirect_to_scheduler": handle_redirect_to_scheduler,
    "done": handle_done,
    "continue": handle_continue,
}


# ============================================================================
# MAIN DISPATCH — replaces the old dispatch_to_layer + all layer classifiers
# ============================================================================

def _handle_abc_pick(state: ConversationState, picked_value: str) -> ConversationState:
    """Handle a direct A/B/C pick from suggestions — save the option, advance to next question."""
    current_q_id = state.get('current_question_id')
    doc_code = state.get('active_document')

    if not current_q_id or current_q_id not in GLOBAL_HARVESTERS:
        state['response'] = "Thanks! What would you like to work on?"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'general_chat'}
        return state

    # Save the picked option by running it through extraction
    original_message = state['user_message']
    state['user_message'] = picked_value
    extracted_count = _extract_facts_from_answer(state)
    state['user_message'] = original_message

    if extracted_count == 0:
        # Extraction failed — save the raw text as the primary fact
        harvester = GLOBAL_HARVESTERS[current_q_id]
        primary_fact = harvester['primary_facts'][0] if harvester['primary_facts'] else None
        if primary_fact:
            save_fact(state['project_id'], primary_fact, picked_value, 'chat')
            state['facts'][primary_fact] = {
                'value': picked_value,
                'source': 'chat',
                'updated_at': datetime.utcnow().isoformat()
            }
            extracted_count = 1

    # Mark question as asked
    if current_q_id not in state['asked_questions']:
        state['asked_questions'].append(current_q_id)
    if current_q_id in state.get('pending_questions', []):
        state['pending_questions'].remove(current_q_id)

    # Advance to next question or ready state
    if doc_code:
        state['pending_questions'] = determine_pending_questions(
            doc_code, state['facts'], state['asked_questions'], state.get('skipped_questions', [])
        )
        readiness = calculate_document_readiness(doc_code, state['facts'])

        if readiness['is_ready']:
            doc_name = get_document_display_name(doc_code)
            state['response'] = (
                f"Saved! ✅ And that's everything I need for your **{doc_name}**! 🎉\n\n"
                f"Would you like me to generate it now?"
            )
            state['active_layer'] = LAYER_GENERATION
            state['current_question_id'] = None
            state['layer_context'] = {'last_action': 'ready_to_generate'}
            return state

        if state['pending_questions']:
            next_q_id = state['pending_questions'][0]
            next_q_text = GLOBAL_HARVESTERS[next_q_id]['question']
            attempts = state.get('question_attempts', {})
            if attempts.get(next_q_id, 0) == 0:
                attempts[next_q_id] = 1
            state['question_attempts'] = attempts
            state['current_question_id'] = next_q_id
            state['response'] = f"Saved! ✅ Next question:\n\n**{next_q_text}**"
            state['active_layer'] = LAYER_QUESTIONING
            state['layer_context'] = {'last_action': 'asked_question', 'question_id': next_q_id}
            return state
        else:
            doc_name = get_document_display_name(doc_code)
            state['response'] = (
                f"Saved! ✅ That's all my questions! Your **{doc_name}** is "
                f"{readiness['required_percentage']:.0f}% complete.\n\n"
                f"I can generate it now — missing parts will be inferred. Ready?"
            )
            state['current_question_id'] = None
            state['active_layer'] = LAYER_GENERATION
            state['layer_context'] = {'last_action': 'ready_to_generate'}
            return state
    else:
        state['response'] = "Saved! ✅ What would you like to do next?"
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {'last_action': 'general_chat'}
        return state


# Comprehensive greeting keyword set — zero LLM, hardcoded intro response
_GREETING_KEYWORDS = {
    # Simple greetings
    'hi', 'hey', 'hello', 'hiya', 'heya', 'howdy', 'yo', 'sup', 'wassup', 'wazzup',
    'greetings', 'gm', 'good morning', 'good afternoon', 'good evening', 'good day','hola',
    # Help / what-can-you-do
    'help', 'help me', 'how can you help', 'how can u help', 'how can you help me',
    'how can u help me', 'what can you do', 'what can u do', 'what do you do',
    'what are you', 'who are you', 'how does this work', 'what is this', 'what is cammi',
    'how do you work', 'how do u work', 'how does cammi work',
    # Getting started
    'start', 'begin', 'let\'s start', 'lets start', 'let\'s begin', 'lets begin',
    'get started', 'let\'s go', 'lets go', 'i\'m ready', 'im ready', 'ready',
    'i want to start', 'i want to begin', 'where do i start', 'where do i begin',
    # Casual openers
    "what's up", 'whats up', 'how are you', 'how r u', 'how are u',
}


def dispatch(state: ConversationState) -> ConversationState:
    """Main dispatch: detect loops, route via unified LLM, execute action handler."""

    # CHANGE 3: Loop detection (DISABLED)
    # This was firing false-positives on normal "Next question" messages and hijacking valid answers.
    # Re-enable only after replacing with a progress-based loop guard.
    # if detect_loop(state):
    #     return break_loop(state)

    # ── ALIGN INTRO PREPEND FLAG (zero LLM) ───────────────────
    # For the first user message in Align, mark intro as shown and set a one-time
    # prepend flag. The normal flow continues and generates the actual reply in the
    # same turn; finalizer prepends this intro once per project.
    _current_tab = (state.get('current_tab') or '').strip().lower()
    print(f"🔍 ALIGN INTRO DEBUG — current_tab={repr(state.get('current_tab'))}, normalized={repr(_current_tab)}, SECTION_ALIGN={repr(SECTION_ALIGN)}, align_intro_shown={repr(state.get('align_intro_shown'))}, tab_match={_current_tab == SECTION_ALIGN}, intro_not_shown={not state.get('align_intro_shown')}")
    if _current_tab == SECTION_ALIGN and not state.get('align_intro_shown'):
        print(f"⚡ ALIGN INTRO FLAG — will prepend intro to first Align reply")
        state['align_intro_shown'] = True
        # Store this outside layer_context because many handlers replace layer_context.
        # Keeping it at top-level ensures we can prepend once in finalize_response.
        state['pending_align_intro_text'] = ALIGN_INTRO_ONCE_TEXT
        update_section_state(
            state['project_id'],
            state['current_tab'],
            state.get('completed_documents', []),
            align_intro_shown=True
        )
    # ── END ALIGN INTRO PREPEND FLAG ──────────────────────────

    # ── GREETING INTERCEPT (zero LLM) ────────────────────────
    # When there is no prior conversation history AND the message is a greeting/help
    # phrase, skip ALL LLM calls and return the hardcoded intro immediately.
    msg_for_greet = state['user_message'].lower().strip().rstrip('!.,?')
    history = state.get('conversation_history', [])
    if not history and msg_for_greet in _GREETING_KEYWORDS:
        print(f"⚡ GREETING INTERCEPT: '{msg_for_greet}' — bypassing LLM")
        state = handle_greet(state, {})
        state['last_agent'] = 'greeting_intercept'
        return state
    # ── END GREETING INTERCEPT ───────────────────────────────

    # ── A/B/C INTERCEPT ──────────────────────────────────────
    # If system just offered suggestions (A, B, C) and user typed exactly
    # a/b/c, skip the router entirely — direct lookup, zero LLM calls.
    lc = state.get('layer_context', {})
    if lc.get('last_action') == 'offered_suggestions':
        msg_clean = state['user_message'].strip().lower().rstrip('.!,')
        option_map = {'a': lc.get('option_a', ''), 'b': lc.get('option_b', ''), 'c': lc.get('option_c', '')}
        if msg_clean in option_map and option_map[msg_clean]:
            picked = option_map[msg_clean]
            print(f"⚡ ABC INTERCEPT: user picked '{msg_clean}' → \"{picked}\"")
            state = _handle_abc_pick(state, picked)
            state['last_agent'] = 'abc_intercept'
            return state
    # ── END A/B/C INTERCEPT ──────────────────────────────────

    # ── FACT VIEW / EDIT KEYWORD INTERCEPT ────────────────────
    # Hardcoded patterns that bypass ALL LLM routing — zero latency.
    msg_lower = state['user_message'].lower().strip()
    _view_exact = {'info', 'facts', 'my info', 'my facts', 'show info', 'show facts', 'view info', 'view facts'}
    _edit_exact = {'edit info', 'edit facts', 'edit my info', 'edit my facts'}
    _view_substr = ['view my info', 'show my info', 'show my facts', 'view my facts',
                    'what do you know about me', 'what do you know about my']
    _edit_substr = ['edit my info', 'edit my facts', 'change my info', 'update my info',
                    'change my facts', 'update my facts']
    _is_edit = msg_lower in _edit_exact or any(kw in msg_lower for kw in _edit_substr)
    _is_view = (not _is_edit) and (msg_lower in _view_exact or any(kw in msg_lower for kw in _view_substr))
    if _is_edit:
        print(f"⚡ KEYWORD INTERCEPT: edit facts")
        state = handle_edit_fact(state, {})
        state['last_agent'] = 'keyword_intercept_edit'
        return state
    if _is_view:
        print(f"⚡ KEYWORD INTERCEPT: view facts")
        state = handle_show_facts(state, {})
        state['last_agent'] = 'keyword_intercept_view'
        return state
    # ── END FACT VIEW / EDIT KEYWORD INTERCEPT ────────────────

    # ── IMPROVE BUTTON EXACT TRIGGER INTERCEPT ─────────────────
    # Frontend sends this exact message when user clicks Improve Now.
    if state.get('user_message', '').strip() == IMPROVE_QUALITY_TRIGGER:
        print("⚡ IMPROVE INTERCEPT: exact frontend trigger received")
        state = handle_improve_document_quality(state, {})
        state['last_agent'] = 'improve_intercept'
        return state

    # Improve flow now auto-applies suggestions; user can directly edit or say generate.
    # ── END IMPROVE BUTTON INTERCEPT ───────────────────────────

    # Campaign / scheduler detection is handled by the LLM prompts
    # (sticky handlers and unified router) — no keyword intercept needed.

    # ── STICKY GENERAL_CHAT CHECK ────────────────────────────
    # When user exited to chat or is post-generation with at least one doc done,
    # stay in general chat until they select a document or done.
    if lc.get('sticky_general_chat') is True:
        print(f"🔄 STICKY GENERAL CHAT: bypassing router")
        return handle_general_chat_sticky(state)
    # ── END STICKY GENERAL_CHAT CHECK ────────────────────────

    # ── STICKY QUESTIONING CHECK ─────────────────────────────
    # When document is locked and we're in questioning, bypass router entirely.
    # Single LLM call handles: extract facts OR route to skip/help/show_facts/etc OR handle general inline.
    if state.get('active_layer') == LAYER_QUESTIONING and state.get('active_document'):
        print(f"🔄 STICKY QUESTIONING: bypassing router, going to questioning layer")
        return handle_questioning_sticky(state)
    # ── END STICKY QUESTIONING CHECK ──────────────────────────

    # ── DISCOVERY vs STICKY GENERAL CHAT (no active document) ─
    # New project (no completed docs) → discovery. After at least one doc generated → general chat.
    if not state.get('active_document') and state.get('active_layer') in [LAYER_DISCOVERY, LAYER_POST_GENERATION]:
        completed = state.get('completed_documents', [])
        if len(completed) == 0:
            print(f"🔄 DISCOVERY: no completed docs, layer={state.get('active_layer')}")
            return handle_discovery_sticky(state)
        # At least one doc completed: route to sticky general chat and keep them there
        lc = dict(state.get('layer_context', {}))
        lc['sticky_general_chat'] = True
        state['layer_context'] = lc
        print(f"🔄 STICKY GENERAL CHAT: has completed docs, layer={state.get('active_layer')}")
        return handle_general_chat_sticky(state)
    # ── END DISCOVERY / GENERAL CHAT ─────────────────────────

    # CHANGE 1+2: Single unified router call
    action = unified_route(state)
    action_name = action.get('action', 'general_chat')

    # Look up and execute handler
    handler = ACTION_HANDLERS.get(action_name)

    if handler:
        state = handler(state, action)
    else:
        print(f"⚠️ Unknown action: {action_name}, falling back to general_chat")
        state = handle_general_chat(state, action)

    state['last_agent'] = f'action_{action_name}'
    return state


# ============================================================================
# RESPONSE FINALIZER (UNCHANGED)
# ============================================================================

def finalize_response(state: ConversationState) -> ConversationState:
    if not state.get('response'):
        state['response'] = "I'm here to help! What would you like to do?"

    pending_align_intro = state.pop('pending_align_intro_text', None)
    if pending_align_intro:
        state['response'] = f"{pending_align_intro}\n\n{state['response']}" if state.get('response') else pending_align_intro

    # Parallelize all three independent DB writes
    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(save_conversation_turn, state['project_id'], 'USER', state['user_message'])
        executor.submit(save_conversation_turn, state['project_id'], 'ASSISTANT', state['response'])
        executor.submit(save_project_state, state)

    state['should_end'] = True
    return state


# ============================================================================
# WORKFLOW (direct dispatch → finalize)
# ============================================================================

def run_workflow(state: ConversationState) -> ConversationState:
    state = dispatch(state)
    state = finalize_response(state)
    return state


# ============================================================================
# SPECIAL COMMANDS (UNCHANGED)
# ============================================================================

def check_special_commands(message: str) -> Optional[str]:
    msg_lower = message.lower().strip()
    if msg_lower in ['!reset', '!clear', '!restart']:
        return 'RESET'
    if msg_lower in ['!debug', '!state', '!status']:
        return 'DEBUG'
    return None


def handle_special_command(command: str, state: ConversationState) -> Optional[ConversationState]:
    if command == 'RESET':
        state['active_layer'] = LAYER_DISCOVERY
        state['layer_context'] = {}
        state['active_document'] = None
        state['current_question_id'] = None
        state['asked_questions'] = []
        state['pending_questions'] = []
        state['question_attempts'] = {}
        state['skipped_questions'] = []
        state['response'] = f"🔄 Session reset! Let's start fresh.\n\n{build_document_list_text(state)}\n\nWhat would you like to create?"
        state['should_end'] = True
        save_project_state(state)
        return state

    if command == 'DEBUG':
        debug = {
            'active_layer': state['active_layer'],
            'layer_context': state['layer_context'],
            'active_document': state['active_document'],
            'current_question_id': state['current_question_id'],
            'asked_questions_count': len(state['asked_questions']),
            'pending_questions_count': len(state['pending_questions']),
            'skipped_questions': state.get('skipped_questions', []),
            'facts_count': len([f for f in state['facts'].values() if f.get('value')]),
            'completed_documents': state['completed_documents'],
            'question_attempts': state.get('question_attempts', {})
        }
        state['response'] = f"🔧 Debug:\n```json\n{json.dumps(debug, indent=2, default=str)}\n```"
        state['should_end'] = True
        return state

    return None


# ============================================================================
# REQUEST VALIDATION (UNCHANGED)
# ============================================================================

def validate_request(event: Dict[str, Any]) -> Tuple[bool, str, Dict[str, Any]]:
    project_id = event.get('project_id')
    session_id = event.get('session_id')
    user_message = event.get('user_message', '').strip()

    if not project_id:
        return False, "Missing required field: project_id", {}
    if not session_id:
        return False, "Missing required field: session_id", {}
    if not user_message:
        return False, "Missing required field: user_message", {}

    user_id = get_user_id_from_session(session_id)
    if not user_id:
        return False, "Invalid session_id", {}

    return True, "", {
        'project_id': project_id,
        'session_id': session_id,
        'user_id': user_id,
        'user_message': user_message
    }


def sanitize_input(message: str) -> str:
    message = message.strip()
    message = ' '.join(message.split())
    if len(message) > 5000:
        message = message[:5000]
    return message


# ============================================================================
# RESPONSE HELPERS (UNCHANGED)
# ============================================================================

def create_success_response(response_text: str, state: ConversationState) -> Dict[str, Any]:
    active_doc_code = state.get('active_document')
    generating_doc_code = state.get('generating_document')
    metadata = {
        'mode': state.get('active_layer', 'DISCOVERY'),
        # Frontend expects lowercase document codes (e.g., "gtm", "kmf")
        'active_document': active_doc_code.lower() if active_doc_code else None,
        'active_document_name': get_document_display_name(active_doc_code) if active_doc_code else None,
        'generating_document': generating_doc_code.lower() if generating_doc_code else None,
        'generating_document_name': get_document_display_name(generating_doc_code) if generating_doc_code else None,
        'current_question_id': state.get('current_question_id'),
        'facts_collected': len([f for f in state.get('facts', {}).values() if f.get('value')]),
        'completed_documents': state.get('completed_documents', []),
        'last_agent': state.get('last_agent', 'unknown'),
        'active_layer': state.get('active_layer', 'DISCOVERY'),
        'current_tab': state.get('current_tab', SECTION_CLARIFY)
    }

    # Always include readiness percentage + derived status/line at top-level in the response body.
    # When no document is selected, these will be null.
    required_percentage = None
    status = None
    line = None

    if active_doc_code:
        readiness = calculate_document_readiness(active_doc_code, state.get('facts', {}))
        metadata['document_readiness'] = {
            'required_percentage': readiness['required_percentage'],
            'is_ready': readiness['is_ready'],
            'missing_required_count': len(readiness['missing_required'])
        }

        required_percentage = readiness['required_percentage']
        if required_percentage is not None:
            if required_percentage < 30:
                status = "weak"
                line = "Add more details to improve the strength and depth of your document."
            elif required_percentage <= 70:
                status = "average"
                line = "Your document is good but could be improved by adding more clarity and relevant details."
            else:
                status = "strong"
                line = "Your document is clear, well-structured, and detailed, showing strong overall quality."
    
    # Build collected_facts and missing_facts arrays for the active document
    collected_facts = []
    missing_facts_list = []
    facts_progress = None
    ai_insight = None

    if active_doc_code:
        all_doc_facts = get_all_facts_for_document(active_doc_code)
        facts_data = state.get('facts', {})
        for fid in all_doc_facts:
            fact_name = FACT_UNIVERSE.get(fid, fid)
            if fid in facts_data and facts_data[fid].get('value'):
                collected_facts.append({
                    'fact_id': fid,
                    'name': fact_name,
                    'value': facts_data[fid]['value']
                })
            else:
                missing_facts_list.append({
                    'fact_id': fid,
                    'name': fact_name
                })
        total = len(collected_facts) + len(missing_facts_list)
        facts_progress = f"{len(collected_facts)}/{total}"
        ai_insight = "The more details you share, the sharper and more tailored your document will be."

    # Calculate isGenerating flag (true only when document generation has started)
    # Check BOTH active_layer and last_action to ensure it only appears in the "Excellent! I'm generating..." message
    layer_context = state.get('layer_context', {})
    last_action = layer_context.get('last_action', '')
    is_generating = (
        state.get('active_layer') == LAYER_POST_GENERATION and 
        last_action == 'document_generated'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'success': True,
            'response': response_text,
            'isGenerating': is_generating,
            'required_percentage': required_percentage,
            'status': status,
            'line': line,
            'collected_facts': collected_facts if collected_facts else None,
            'missing_facts': missing_facts_list if missing_facts_list else None,
            'facts_progress': facts_progress,
            'ai_insight': ai_insight,
            'metadata': metadata
        }),
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
    }


def create_error_response(error_message: str, user_friendly: bool = True) -> Dict[str, Any]:
    friendly = "I encountered a small hiccup. Could you try again?" if user_friendly else error_message
    return {
        'statusCode': 500,
        'body': json.dumps({'success': False, 'error': error_message, 'response': friendly}),
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
    }


def create_validation_error_response(error_message: str) -> Dict[str, Any]:
    return {
        'statusCode': 400,
        'body': json.dumps({'success': False, 'error': error_message, 'response': None}),
        'headers': {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*'}
    }


# ============================================================================
# LAMBDA HANDLER (UNCHANGED)
# ============================================================================

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    print(f"Received event: {json.dumps(event)}")

    try:
        if 'body' in event:
            if isinstance(event['body'], str):
                try:
                    parsed_body = json.loads(event['body'])
                except json.JSONDecodeError:
                    return create_validation_error_response("Invalid JSON in request body")
            else:
                parsed_body = event['body']
        else:
            parsed_body = event

        is_valid, error_message, extracted_data = validate_request(parsed_body)
        if not is_valid:
            print(f"Validation failed: {error_message}")
            return create_validation_error_response(error_message)

        project_id = extracted_data['project_id']
        session_id = extracted_data['session_id']
        user_id = extracted_data['user_id']
        user_message = sanitize_input(extracted_data['user_message'])

        print(f"Processing: {project_id} - {user_message[:100]}...")

        state = build_initial_state(project_id, user_id, session_id, user_message)
        state = recover_state(state)

        special = check_special_commands(user_message)
        if special:
            result = handle_special_command(special, state)
            if result:
                return create_success_response(result['response'], result)

        print(f"Running workflow: layer={state['active_layer']}")
        final_state = run_workflow(state)
        print(f"Complete. Agent: {final_state.get('last_agent')} | Layer: {final_state.get('active_layer')}")

        return create_success_response(final_state.get('response', ''), final_state)

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return create_error_response(str(e), user_friendly=True)


# ============================================================================
# UTILITY EXPORTS (UNCHANGED)
# ============================================================================

def process_message(project_id: str, session_id: str, user_message: str) -> Dict[str, Any]:
    event = {'project_id': project_id, 'session_id': session_id, 'user_message': user_message}
    response = lambda_handler(event, None)
    return json.loads(response['body'])


def get_project_summary(project_id: str) -> Dict[str, Any]:
    state = load_project_state(project_id)
    facts = load_facts(project_id)
    summary = {
        'project_id': project_id,
        'active_layer': state.get('active_layer', 'UNKNOWN'),
        'active_document': state.get('active_document'),
        'completed_documents': state.get('completed_documents', []),
        'facts_collected': len([f for f in facts.values() if f.get('value')]),
        'document_readiness': {}
    }
    for doc_code in DOCUMENT_REQUIREMENTS:
        readiness = calculate_document_readiness(doc_code, facts)
        summary['document_readiness'][doc_code] = {
            'name': readiness['doc_name'],
            'required_percentage': readiness['required_percentage'],
            'is_ready': readiness['is_ready']
        }
    return summary

