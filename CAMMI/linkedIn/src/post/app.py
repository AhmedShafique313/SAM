import json
import urllib3
import boto3
import base64
import datetime
from datetime import timedelta, timezone
 
http = urllib3.PoolManager()
 
# DynamoDB tables
dynamodb = boto3.resource("dynamodb")
user_table = dynamodb.Table("linkedin-user-table")
post_table = dynamodb.Table("linkedin-posts-table") 
 
 
def lambda_handler(event, context):
    # Parse incoming request body
    body = json.loads(event.get("body", "{}"))
    sub = body.get("sub")
    message = body.get("post_message")
    images = body.get("images", [])   # optional, list of {"image": "...", "filename": "..."}
 
    if not sub or not message:
        return {
            "statusCode": 400,
            "headers": cors_headers(),
            "body": json.dumps({"error": "sub and post_message are required"})
        }
 
    # Get LinkedIn details from linkedin_user_table
    response = user_table.get_item(Key={"sub": sub})
    if "Item" not in response:
        return {
            "statusCode": 404,
            "headers": cors_headers(),
            "body": json.dumps({"error": "User not found in database"})
        }
 
    item = response["Item"]
    sub = item.get("sub")
    access_token = item.get("access_token")
 
    if not sub or not access_token:
        return {
            "statusCode": 400,
            "headers": cors_headers(),
            "body": json.dumps({"error": "Sub or access token missing"})
        }
 
    uploaded_assets = []
 
    # 🔹 Handle multiple images (if any)
    for img in images:
        image_b64 = img.get("image")
        filename = img.get("filename", "upload.png")
 
        if not image_b64:
            continue
 
        image_bytes = base64.b64decode(image_b64)
        mimetype = "image/png" if filename.lower().endswith("png") else "image/jpeg"
 
        # Step 1: Register upload
        register_url = "https://api.linkedin.com/v2/assets?action=registerUpload"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        payload = {
            "registerUploadRequest": {
                "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                "owner": f"urn:li:person:{sub}",
                "serviceRelationships": [
                    {
                        "relationshipType": "OWNER",
                        "identifier": "urn:li:userGeneratedContent"
                    }
                ]
            }
        }
        reg_res = http.request("POST", register_url, body=json.dumps(payload), headers=headers)
        reg_data = json.loads(reg_res.data.decode())
 
        upload_url = reg_data["value"]["uploadMechanism"]["com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"]["uploadUrl"]
        asset = reg_data["value"]["asset"]
 
        # Step 2: Upload binary image
        upload_headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": mimetype,
        }
        http.request("PUT", upload_url, body=image_bytes, headers=upload_headers)
 
        uploaded_assets.append({"asset": asset, "filename": filename})
 
    # Step 3: Create LinkedIn post (with or without images)
    post_url = "https://api.linkedin.com/v2/ugcPosts"
    post_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }
 
    share_media_category = "IMAGE" if uploaded_assets else "NONE"
 
    post_payload = {
        "author": f"urn:li:person:{sub}",
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": message},
                "shareMediaCategory": share_media_category,
                "media": []
            }
        },
        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"}
    }
 
    for img in uploaded_assets:
        post_payload["specificContent"]["com.linkedin.ugc.ShareContent"]["media"].append(
            {
                "status": "READY",
                "media": img["asset"],
                "description": {"text": "Uploaded via Lambda"},
                "title": {"text": img["filename"]},
            }
        )
 
    response = http.request("POST", post_url, body=json.dumps(post_payload), headers=post_headers)
 
    # ✅ Store post details in linkedin-text-post only if successful
    if response.status == 201:
        response_data = json.loads(response.data.decode("utf-8"))
        post_urn = response_data.get("id", "N/A")  # urn:li:share:xxxx
 
        # --- UTC+5 timestamp ---
        tz_plus_5 = timezone(timedelta(hours=5))
        post_time = datetime.datetime.now(tz_plus_5).isoformat()
 
        post_link = f"https://www.linkedin.com/feed/update/{post_urn}"
 
        post_table.put_item(
            Item={
                "sub": sub,
                "post_time": post_time,
                "message": message,
                "post_urn": post_urn,
                "post_url": post_link,
                "image_urls": [img["asset"] for img in uploaded_assets] if uploaded_assets else [],
                "status_code": response.status,
                "status": "posted"
            }
        )
 
    return {
        "statusCode": response.status,
        "headers": cors_headers(),
        "body": response.data.decode("utf-8")
    }
 
 
def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }
 