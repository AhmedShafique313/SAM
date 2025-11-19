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
    "What is the spokesperson’s title or role?",
    "Can you provide links to your brand’s visual assets (e.g., logo, product screenshots)?"
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
        elif document_type.lower() == 'kmf':
            question_list = kmf
        elif document_type.lower() == 'sr':
            question_list = sr  
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

