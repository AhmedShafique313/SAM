import json
import urllib3
import boto3
import base64
import time
import datetime
from datetime import timedelta, timezone

http = urllib3.PoolManager()

# DynamoDB tables
dynamodb = boto3.resource("dynamodb")
user_table = dynamodb.Table("linkedin-user-table")
post_table = dynamodb.Table("linkedin-posts-table")


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body", "{}"))
        sub = body.get("sub")
        message = body.get("post_message")
        images = body.get("images", [])

        if not sub or not message:
            return _resp(400, {"error": "sub and post_message are required"})

        # --- Fetch user ---
        user_res = user_table.get_item(Key={"sub": sub})
        if "Item" not in user_res:
            return _resp(404, {"error": "User not found"})

        user = user_res["Item"]
        access_token = user.get("access_token")

        # IMPORTANT: sub must be LinkedIn member id
        linkedin_member_id = user.get("linkedin_member_id", sub)

        if not access_token or not linkedin_member_id:
            return _resp(400, {"error": "LinkedIn credentials missing"})

        uploaded_assets = []

        # ---------- IMAGE UPLOAD ----------
        for img in images:
            image_b64 = img.get("image")
            filename = img.get("filename", "upload.jpg")

            if not image_b64:
                continue

            # handle data:image/...;base64,
            if "," in image_b64:
                image_b64 = image_b64.split(",", 1)[1]

            try:
                image_bytes = base64.b64decode(image_b64)
            except Exception:
                return _resp(400, {"error": "Invalid base64 image"})

            mimetype = "image/png" if filename.lower().endswith("png") else "image/jpeg"

            # Step 1: Register upload
            reg_payload = {
                "registerUploadRequest": {
                    "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                    "owner": f"urn:li:person:{linkedin_member_id}",
                    "serviceRelationships": [
                        {
                            "relationshipType": "OWNER",
                            "identifier": "urn:li:userGeneratedContent"
                        }
                    ]
                }
            }

            reg_res = http.request(
                "POST",
                "https://api.linkedin.com/v2/assets?action=registerUpload",
                body=json.dumps(reg_payload),
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
            )

            if reg_res.status != 200:
                return _resp(reg_res.status, {"error": "Asset registration failed"})

            reg_data = json.loads(reg_res.data.decode())
            upload_url = reg_data["value"]["uploadMechanism"][
                "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"
            ]["uploadUrl"]
            asset = reg_data["value"]["asset"]

            # Step 2: Upload binary
            upload_res = http.request(
                "PUT",
                upload_url,
                body=image_bytes,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": mimetype
                }
            )

            if upload_res.status not in (200, 201, 202):
                return _resp(upload_res.status, {"error": "Image upload failed"})

            uploaded_assets.append({"asset": asset, "filename": filename})

            # 🔴 REQUIRED delay
            time.sleep(2)

        # ---------- CREATE POST ----------
        share_content = {
            "shareCommentary": {"text": message},
            "shareMediaCategory": "IMAGE" if uploaded_assets else "NONE"
        }

        if uploaded_assets:
            share_content["media"] = [
                {
                    "status": "READY",
                    "media": img["asset"],
                    "title": {"text": img["filename"]}
                }
                for img in uploaded_assets
            ]

        post_payload = {
            "author": f"urn:li:person:{linkedin_member_id}",
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": share_content
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            }
        }

        post_res = http.request(
            "POST",
            "https://api.linkedin.com/v2/ugcPosts",
            body=json.dumps(post_payload),
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0"
            }
        )

        if post_res.status != 201:
            return _resp(post_res.status, {
                "error": "Post creation failed",
                "details": post_res.data.decode()
            })

        post_data = json.loads(post_res.data.decode())
        post_urn = post_data.get("id")

        # ---------- STORE IN DB ----------
        tz_plus_5 = timezone(timedelta(hours=5))
        post_time = datetime.datetime.now(tz_plus_5).isoformat()

        post_table.put_item(
            Item={
                "sub": sub,
                "post_time": post_time,
                "message": message,
                "post_urn": post_urn,
                "post_url": f"https://www.linkedin.com/feed/update/{post_urn}",
                "image_assets": [i["asset"] for i in uploaded_assets],
                "status": "published",
                "status_code": post_res.status
            }
        )

        return _resp(201, {"post_urn": post_urn})

    except Exception as e:
        return _resp(500, {"error": str(e)})


def _resp(status, body):
    return {
        "statusCode": status,
        "headers": cors_headers(),
        "body": json.dumps(body)
    }


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "OPTIONS,POST",
        "Access-Control-Allow-Headers": "Content-Type,Authorization"
    }
