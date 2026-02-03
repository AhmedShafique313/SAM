import json
import boto3
import urllib3
import base64
import time
import datetime
import uuid
from datetime import timedelta, timezone
from boto3.dynamodb.conditions import Key

# ---------- AWS Clients ----------
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
http = urllib3.PoolManager()

# ---------- Tables / Bucket ----------
POSTS_TABLE = "posts-table"
LINKEDIN_TABLE = "linkedin-posts-table"
USER_TABLE = "linkedin-user-table"
IMAGE_BUCKET = "cammi-devprod"

posts_table = dynamodb.Table(POSTS_TABLE)
linkedin_table = dynamodb.Table(LINKEDIN_TABLE)
user_table = dynamodb.Table(USER_TABLE)


def normalize_hashtags(raw):
    hashtags = []
    for h in raw or []:
        if isinstance(h, str):
            tag = h.strip()
        else:
            continue

        if tag.lower().startswith("hashtag#"):
            tag = tag[8:].strip()

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
        campaign_id = body.get("campaign_id")
        sub = body.get("sub")

        if not post_id or not sub or not campaign_id:
            return response(400, "post_id, campaign_id and sub are required")

        # ---------- DEFAULT NULL SAFE VALUES ----------
        best_post_day = body.get("best_post_day")
        best_post_time = body.get("best_post_time")
        description = body.get("description")
        generated_at = body.get("generated_at")
        hashtags = body.get("hashtags")
        image_generation_prompt = body.get("image_generation_prompt")
        scheduled_time = body.get("scheduled_time")
        status = "published"
        title = body.get("title")

        images = body.get("images", [])

        # ---------- Upload Images to S3 ----------
        image_keys = []

        for img_b64 in images:
            if not img_b64:
                continue

            if "," in img_b64:
                img_b64 = img_b64.split(",", 1)[1]

            image_bytes = base64.b64decode(img_b64)
            image_id = str(uuid.uuid4())
            s3_key = f"images/{post_id}/{image_id}.jpg"

            s3.put_object(
                Bucket=IMAGE_BUCKET,
                Key=s3_key,
                Body=image_bytes,
                ContentType="image/jpeg"
            )

            image_keys.append({"s3_key": s3_key})

        # ---------- UPDATE posts-table FIRST ----------
        posts_table.update_item(
            Key={
                "post_id": post_id,
                "campaign_id": campaign_id
            },
            UpdateExpression="""
                SET best_post_day = :bpd,
                    best_post_time = :bpt,
                    description = :desc,
                    generated_at = :ga,
                    hashtags = :ht,
                    image_generation_prompt = :igp,
                    scheduled_time = :st,
                    #status = :status,
                    title = :title,
                    image_keys = :ik
            """,
            ExpressionAttributeNames={
                "#status": "status"
            },
            ExpressionAttributeValues={
                ":bpd": best_post_day,
                ":bpt": best_post_time,
                ":desc": description,
                ":ga": generated_at,
                ":ht": hashtags,
                ":igp": image_generation_prompt,
                ":st": scheduled_time,
                ":status": status,
                ":title": title,
                ":ik": image_keys
            }
        )

        # ---------- Fetch Post (unchanged flow) ----------
        post_resp = posts_table.get_item(
            Key={"post_id": post_id, "campaign_id": campaign_id}
        )

        if "Item" not in post_resp:
            return response(404, "Post not found")

        post_item = post_resp["Item"]

        title = (post_item.get("title") or "").strip()
        description = (post_item.get("description") or "").strip()
        raw_hashtags = post_item.get("hashtags", [])
        hashtags = normalize_hashtags(raw_hashtags)

        if hashtags:
            message = f"{title}\n\n{description}\n{' '.join(hashtags)}"
        else:
            message = f"{title}\n\n{description}"

        # ---------- Current Time (UTC+5) ----------
        tz_plus_5 = timezone(timedelta(hours=5))
        current_time = datetime.datetime.now(tz_plus_5).isoformat()

        # ---------- Fetch LinkedIn User ----------
        user_resp = user_table.get_item(Key={"sub": sub})
        if "Item" not in user_resp:
            return response(404, "LinkedIn user not found")

        user = user_resp["Item"]
        access_token = user.get("access_token")
        linkedin_member_id = user.get("linkedin_member_id", sub)

        if not access_token:
            return response(400, "LinkedIn credentials missing")

        # ---------- Upload Images to LinkedIn ----------
        uploaded_assets = []

        for img in image_keys:
            s3_obj = s3.get_object(
                Bucket=IMAGE_BUCKET,
                Key=img["s3_key"]
            )
            image_bytes = s3_obj["Body"].read()

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

            reg_data = json.loads(reg_res.data.decode())
            upload_url = reg_data["value"]["uploadMechanism"][
                "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"
            ]["uploadUrl"]
            asset = reg_data["value"]["asset"]

            http.request(
                "PUT",
                upload_url,
                body=image_bytes,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "image/jpeg"
                }
            )

            uploaded_assets.append({
                "asset": asset,
                "filename": img["s3_key"].split("/")[-1]
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

        post_urn = json.loads(post_res.data.decode()).get("id")

        # ---------- linkedin-posts-table ----------
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
