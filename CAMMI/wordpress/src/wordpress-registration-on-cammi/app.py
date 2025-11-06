import json, uuid, boto3
from botocore.exceptions import ClientError
 
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Wordpress-sites-table')
 
def lambda_handler(event, context):
    # CORS Policy
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,POST"
    }
 
    if "body" in event:
        body = json.loads(event["body"])
    else:
        body = event
   
    sitename = body.get("sitename")
    baseurl = body.get("baseurl")
    username = body.get("username")
    app_password = body.get("app_password")
 
 
    # validation if user providing all the required details
    if not all([sitename, baseurl, username, app_password]):
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps({"error": "Missing required fields"})
        }
 
    site_id = str(uuid.uuid4())
 
    # store in dynamodb
   
    item = {
        "sitename": sitename,   # partition key
        "id": site_id,
        "base_url": baseurl.rstrip("/"),
        "username": username,
        "app_password": app_password
    }
    table.put_item(Item=item)
 
    # SUCCESS rESPONSE
    return {
        "statusCode": 201,
        "headers": headers,
        "body": json.dumps({
            "message": "✅ Site registered successfully!",
            "id": site_id,
            "sitename": sitename
        })
    }