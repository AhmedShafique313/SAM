import json
import urllib3
import boto3
import time
from datetime import datetime, timezone, timedelta

http = urllib3.PoolManager()
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")

user_table = dynamodb.Table("linkedin-user-table")
post_table = dynamodb.Table("linkedin-posts-table")

BUCKET = "cammi-devprod"
PKT = timezone(timedelta(hours=5))


def lambda_handler(event, context):
    try:
        body = event if isinstance(event, dict) else json.loads(event)

        sub = body.get("sub")
        message = body.get("message")
        scheduled_time = body.get("scheduled_time")  # 🔑 EXACT KEY
        image_keys = body.get("image_keys", [])

        if not sub or not message or not scheduled_time:
            return _resp(400, {"error": "Invalid payload"})

        user = user_table.get_item(Key={"sub": sub}).get("Item")
        if not user or not user.get("access_token"):
            return _resp(400, {"error": "Access token missing"})

        linkedin_id = user.get("linkedin_member_id", sub)
        access_token = user["access_token"]

        media_assets = []

        # ---------- IMAGE UPLOAD ----------
        for key in image_keys:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            image_bytes = obj["Body"].read()

            # Register
            reg_payload = {
                "registerUploadRequest": {
                    "owner": f"urn:li:person:{linkedin_id}",
                    "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                    "serviceRelationships": [{
                        "relationshipType": "OWNER",
                        "identifier": "urn:li:userGeneratedContent"
                    }]
                }
            }

            reg = http.request(
                "POST",
                "https://api.linkedin.com/v2/assets?action=registerUpload",
                body=json.dumps(reg_payload),
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }
            )

            reg_data = json.loads(reg.data.decode())
            upload_url = reg_data["value"]["uploadMechanism"][
                "com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"
            ]["uploadUrl"]
            asset = reg_data["value"]["asset"]

            # Upload binary (MUST be PUT)
            http.request(
                "PUT",
                upload_url,
                body=image_bytes,
                headers={
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "image/jpeg"
                }
            )

            media_assets.append({
                "status": "READY",
                "media": asset
            })

            time.sleep(2)

        # ---------- CREATE POST ----------
        share_content = {
            "shareCommentary": {"text": message},
            "shareMediaCategory": "IMAGE" if media_assets else "NONE"
        }

        if media_assets:
            share_content["media"] = media_assets

        payload = {
            "author": f"urn:li:person:{linkedin_id}",
            "lifecycleState": "PUBLISHED",
            "specificContent": {
                "com.linkedin.ugc.ShareContent": share_content
            },
            "visibility": {
                "com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"
            }
        }

        resp = http.request(
            "POST",
            "https://api.linkedin.com/v2/ugcPosts",
            body=json.dumps(payload),
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "X-Restli-Protocol-Version": "2.0.0"
            }
        )

        status = "published" if resp.status == 201 else "failed"
        post_urn = None

        if resp.status == 201:
            post_urn = json.loads(resp.data.decode()).get("id")

        # 🔑 UPDATE USING EXACT SAME SORT KEY
        post_table.update_item(
            Key={
                "sub": sub,
                "post_time": scheduled_time
            },
            UpdateExpression="SET #s=:s, post_urn=:u, status_code=:c",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":s": status,
                ":u": post_urn or "N/A",
                ":c": resp.status
            }
        )

        return _resp(200, {"status": status, "post_urn": post_urn})

    except Exception as e:
        return _resp(500, {"error": str(e)})


def _resp(code, body):
    return {
        "statusCode": code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }
