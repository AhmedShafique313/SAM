import json
import boto3
import urllib3
import base64
import time
import datetime
from datetime import timedelta, timezone
from boto3.dynamodb.conditions import Key

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
http = urllib3.PoolManager()

# ---------- Tables ----------
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"
USER_TABLE = "linkedin-user-table"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)
user_table = dynamodb.Table(USER_TABLE)


def normalize_hashtags(raw):
    """
    Normalize hashtags from DynamoDB and ensure LinkedIn-safe format:
    - Removes any "hashtag#" prefix
    - Ensures '#' at start
    - Returns a list of clean hashtags
    """
    hashtags = []
    for h in raw or []:
        if isinstance(h, dict) and "S" in h:
            tag = h["S"].strip()
        elif isinstance(h, str):
            tag = h.strip()
        else:
            continue

        # remove any "hashtag#" prefix
        if tag.lower().startswith("hashtag#"):
            tag = tag[8:].strip()

        # ensure it starts with #
        if not tag.startswith("#"):
            tag = f"#{tag}"

        hashtags.append(tag)
    return hashtags


def lambda_handler(event, context):
    try:
        # ---------- CORS ----------
        if event.get("httpMethod") == "OPTIONS":
            return response(200, {})

        # ---------- Input ----------
        body = json.loads(event.get("body", "{}"))
        post_id = body.get("post_id")
        sub = body.get("sub")

        if not post_id or not sub:
            return response(400, "post_id and sub are required")

        # ---------- Fetch Post ----------
        post_resp = posts_table.query(
            KeyConditionExpression=Key("post_id").eq(post_id)
        )

        if not post_resp.get("Items"):
            return response(404, "Post not found")

        post_item = post_resp["Items"][0]

        campaign_id = post_item["campaign_id"]
        image_keys = post_item.get("image_keys", [])
        title = post_item.get("title", "").strip()
        description = post_item.get("description", "").strip()

        # ---------- Hashtags ----------
        raw_hashtags = post_item.get("hashtags", [])
        hashtags = normalize_hashtags(raw_hashtags)

        if hashtags:
            hashtag_line = " ".join(hashtags)
            # ONE newline only — LinkedIn requirement
            message = f"{title}\n\n{description}\n{hashtag_line}"
        else:
            message = f"{title}\n\n{description}"

        # ---------- Current Time (UTC+5) ----------
        tz_plus_5 = timezone(timedelta(hours=5))
        current_time = datetime.datetime.now(tz_plus_5).isoformat()

        # ---------- Update posts-table ----------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="SET scheduled_time = :st, #status = :status",
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":st": current_time,
                ":status": "published"
            }
        )

        # ---------- Fetch LinkedIn User ----------
        user_resp = user_table.get_item(Key={"sub": sub})
        if "Item" not in user_resp:
            return response(404, "LinkedIn user not found")

        user = user_resp["Item"]
        access_token = user.get("access_token")
        linkedin_member_id = user.get("linkedin_member_id", sub)

        if not access_token or not linkedin_member_id:
            return response(400, "LinkedIn credentials missing")

        # ---------- Upload Images ----------
        uploaded_assets = []

        for img in image_keys:
            image_b64 = img.get("image")
            filename = img.get("filename", "upload.jpg")

            if not image_b64:
                continue

            if "," in image_b64:
                image_b64 = image_b64.split(",", 1)[1]

            image_bytes = base64.b64decode(image_b64)
            mimetype = "image/png" if filename.lower().endswith("png") else "image/jpeg"

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
                return response(500, "Image registration failed")

            reg_data = json.loads(reg_res.data.decode())
            upload_url = reg_data["value"]["uploadMechanism"][
                "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"
            ]["uploadUrl"]
            asset = reg_data["value"]["asset"]

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
                return response(500, "Image upload failed")

            uploaded_assets.append({
                "asset": asset,
                "filename": filename
            })

            time.sleep(2)

        # ---------- Create LinkedIn Post ----------
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
            return response(500, post_res.data.decode())

        post_urn = json.loads(post_res.data.decode()).get("id")

        # ---------- Update linkedin-posts-table ----------
        linkedin_table.put_item(
            Item={
                "sub": sub,
                "post_time": current_time,
                "post_id": post_id,
                "campaign_id": campaign_id,
                "message": message,
                "image_keys": image_keys,
                "post_urn": post_urn,
                "post_url": f"https://www.linkedin.com/feed/update/{post_urn}",
                "status": "published"
            }
        )

        return response(200, {
            "message": "Post published successfully",
            "post_time": current_time,
            "post_url": f"https://www.linkedin.com/feed/update/{post_urn}"
        })

    except Exception as e:
        return response(500, str(e))


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
