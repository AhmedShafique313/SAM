import json
import boto3
from boto3.dynamodb.conditions import Key
 
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("project-questions-table")

# Hardcoded questions
icp  = [
    "What is the name of your business?",
    "How would you describe your business concept in a few sentences?",
    "Who is your main customer right now?",
    "What are the top 3 goals you want to hit in the next 12 months?",
    "What challenges are you facing in finding or converting customers?",
    "Do you already know your best-fit customers or industries?",
    "Who are your main competitors?"
]
messaging = [
    "What's the name of the brand we're talking about?",
    "In a nutshell, what are you offering, and what specific headache does it actually cure for people?",
    "Who usually buys this? I'm thinking specific industries, job titles, or company sizes.",
    "Deep down, what's the main mission here? What specific problem are you waking up to solve every day?",
    "Where do you see this going? Like, what's the dream scenario for the business down the road?",
    "What's that one thing that makes you totally different from everyone else doing this?"
]

brand = [
    "How would you explain what the business does in plain English?",
    "Who are we trying to get the attention of? What kind of people or companies are your 'people'?",
    "Why does this brand exist, and where are you hoping to take it eventually?",
    "If someone lined you up next to your competitors, what makes you stand out?",
    "How should the brand sound or feel? Also, are there any vibes you absolutely hate and want to avoid?"
]	

mr = [
    "How would you pitch what you do and who it's for if we were just chatting casually?",
    "What's keeping your ideal customers up at night lately? What are their biggest annoyances?",
    "How are they trying to fix that stuff right now? Are they using other tools, vendors, or just messy workarounds?",
    "Why aren't the current options working for them, and how is your way better?",
    "Where can you realistically take on clients over the next year or two? Is it global or specific regions?",
    "What does an average customer usually spend with you?",
    "Do you have a feel for how many potential customers are actually out there for this?",
    "Who are you competing against, and are you seeing any big shifts in the market recently?"
]
kmf = [
    "What is the name of your business?",
    "Which industry does your business belong to?",
    "What is the main objective or goal of your business?",
    "How would you describe your business concept in a few sentences?",
    "Who is your target market or ideal customer?",
    "What is your short value proposition (how you help customers in a simple way)?",
    "What is the long-term vision for your business?",
    "What key problems does your business solve for customers?",
    "What are the core products or services your business offers?",
    "What makes your business unique compared to competitors?",
    "What tone or personality should your brand convey (e.g., professional, friendly, innovative)?",
    "Are there any additional themes or values you want associated with your brand?"
]

sr = [
    "What is the name of your business?",
    "How would you describe your business concept in a few sentences?",
    "What is the main objective or goal of your business?",
    "What is your value proposition (the main benefit your platform offers customers)?",
    "Which industry does your business belong to?",
    "What is your business model (e.g., subscription, pay-per-download, freemium)?",
    "Who is your target market or ideal customer?",
    "What is your primary geographic market focus?",
    "How do you position your pricing (e.g., low-tier, mid-tier, premium)?",
    "Who are your main competitors?",
    "What is your estimated marketing budget?",
    "What stage of development is your business currently in?",
    "What are your top user development priorities?",
    "What are your key marketing objectives?",
    "When do you plan to start this project?",
    "When is your planned end date or long-term milestone?"
]
smp = [
    "How would you describe the business in just a sentence or two?",
    "How does the money-making side work? Like pricing models, how you sell, that sort of thing.",
    "Who's the target buyer here? Any specific industries, company sizes, or roles?",
    "What is the number one problem you are helping people solve right now?",
    "What's the big long-term vision you're building toward?",
    "Fast forward a few years—what does 'success' look like to you for this business?"
]

icp2 = [
    "Give me the quick scoop on the business—what you do and the big issue you fix.",
    "Who is the primary decision-maker you want to target? What's their job title and what are they responsible for?",
    "What kind of pressure is this person under? What goals are they stressed about hitting?",
    "Where does this person usually work? I'm curious about company size, industry, location, etc.",
    "Where do they hang out online or get their info? Specific blogs, communities, or influencers?"
]

bs = [
    "What is the name of your business?",
    "How would you describe your business concept in a few sentences?",
    "What is your value proposition (the main benefit your platform offers customers)?",
    "Which industry does your business belong to?",
    "Who is your target market or ideal customer?",
    "What is your primary geographic market focus?",
    "How do you position your pricing (e.g., low-tier, mid-tier, premium)?",
    "Who are your main competitors?",
    "When do you plan to start this project?",
    "What is your long-term end date or milestone?",
    "Which customers are approved to be publicly showcased?",
    "Can you provide links to approved customer video assets?",
    "Can you provide links to approved customer case studies?",
    "What are the approved customer quotes you want to feature?",
    "Can you provide links to approved customer logos or other visual assets?",
    "What brag points or achievements would you like to highlight?",
    "Who will act as the spokesperson for your business?",
    "What is the spokesperson's title or role?",
    "Can you provide links to your brand's visual assets (e.g., logo, product screenshots)?"
]

gtm = [
    "What do you want to accomplish in one year?",
    "Where do you want to be in three years?",
    "Where is your short term focus?",
    "How would you describe your business concept in a few sentences?",
    "Tell us about who you sell to? Where are they located?",
    "What is unique about your business?",
    "What marketing tools do you have available to you?",
    "Who are your main competitors?",
    "What are your strengths, weaknesses, opps and threats?",
    "Tell us about your product/solution/service?"
]


 
def lambda_handler(event, context):
    try:
        # ✅ Read headers
        headers = event.get("headers", {})
        project_id = headers.get("project_id")
        document_type = headers.get("document_type")
 
        if not project_id or not document_type:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*",  
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
                },
                "body": json.dumps({"error": "Missing project_id or document_type in headers"})
            }
 
        # ✅ Select predefined questions based on document_type
        if document_type.lower() == 'icp':
            question_list = icp
        elif document_type.lower() == 'gtm': 
            question_list = gtm
        elif document_type.lower() == 'brand':
            question_list = brand
        elif document_type.lower() == 'messaging':
            question_list = messaging    
        elif document_type.lower() == 'kmf':
            question_list = kmf
        elif document_type.lower() == 'mr':
            question_list = mr
        elif document_type.lower() == 'sr':
            question_list = sr  
        elif document_type.lower() == 'smp':
            question_list = smp
        elif document_type.lower() == 'icp2':
            question_list = icp2
        elif document_type.lower() == 'bs':
            question_list = bs             
        else:
            return {
                "statusCode": 400,
                "headers": {
                    "Access-Control-Allow-Origin": "*",  
                    "Access-Control-Allow-Headers": "*",
                    "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
                },
                "body": json.dumps({"error": "Invalid document_type"})
            }
 
        # ✅ Query DynamoDB for project_id
        response = table.query(
            KeyConditionExpression=Key("project_id").eq(project_id)
        )
        items = response.get("Items", [])
 
        if items:
            # Extract all question_text from DB
            db_questions = {item["question_text"] for item in items}
 
            # Find missing questions
            missing_questions = [
                q for q in question_list if q not in db_questions
            ]
        else:
            # If no data for this project, return all predefined
            missing_questions = question_list
 
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",  
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({
                "missing_questions": missing_questions,
                "document_type": document_type
            })
        }
 
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET"
            },
            "body": json.dumps({"error": str(e)})
        }

