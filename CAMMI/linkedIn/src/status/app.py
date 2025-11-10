import json
import urllib3
import boto3
from datetime import datetime, timezone, timedelta

http = urllib3.PoolManager()
dynamodb = boto3.resource("dynamodb")
user_table = dynamodb.Table("linkedin-user-table")
post_table = dynamodb.Table("linkedin-posts-table")
s3 = boto3.client("s3")

# Pakistan Standard Time (UTC+5)
PKT = timezone(timedelta(hours=5))
BUCKET_NAME = "cammi-devprod"


def lambda_handler(event, context):
    try:
        # EventBridge sends payload directly
        body = event.get("body")
        if body:
            body = json.loads(body) if isinstance(body, str) else body
        else:
            body = event  # EventBridge payload

        # Extract required fields
        sub = body.get("sub")
        message = body.get("message")
        scheduled_time_str = body.get("scheduled_time")
        status = body.get("status", "pending")
        post_time_str = body.get("post_time", scheduled_time_str)
        image_keys = body.get("image_keys", [])  # Expect S3 keys like ["sub/test1.png"]

        if not sub or not message or not scheduled_time_str:
            return _response(400, {"error": "sub, message, and scheduled_time are required"})

        # Parse times
        scheduled_time = datetime.fromisoformat(scheduled_time_str)
        post_time = datetime.fromisoformat(post_time_str)

        # Fetch user access token
        user = user_table.get_item(Key={"sub": sub}).get("Item")
        if not user or not user.get("access_token"):
            return _response(400, {"error": f"Missing access token for {sub}"})

        # Compare current PKT time with scheduled_time
        now_pkt = datetime.now(PKT)
        if status == "pending" and now_pkt >= scheduled_time:
            result = _post_to_linkedin(sub, message, post_time, user["access_token"], image_keys)
        else:
            result = {"status": "skipped", "reason": f"Scheduled time not reached: {scheduled_time_str}"}

        return _response(200, result)

    except Exception as e:
        return _response(500, {"error": str(e)})


def _post_to_linkedin(sub, message, post_time, access_token, image_keys=None):
    media_assets = []

    if image_keys:
        for key in image_keys:
            try:
                # 1️⃣ Download image from S3
                obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
                image_bytes = obj["Body"].read()

                # 2️⃣ Register upload with LinkedIn
                register_payload = {
                    "registerUploadRequest": {
                        "owner": f"urn:li:person:{sub}",
                        "recipes": ["urn:li:digitalmediaRecipe:feedshare-image"],
                        "serviceRelationships": [{
                            "relationshipType": "OWNER",
                            "identifier": "urn:li:userGeneratedContent"
                        }]
                    }
                }
                headers = {
                    "Authorization": f"Bearer {access_token}",
                    "Content-Type": "application/json"
                }

                reg_resp = http.request(
                    "POST",
                    "https://api.linkedin.com/v2/assets?action=registerUpload",
                    body=json.dumps(register_payload),
                    headers=headers
                )

                reg_data = json.loads(reg_resp.data.decode())
                upload_url = reg_data["value"]["uploadMechanism"]["com.linkedin.digitalmedia.uploading.MediaUploadHttpRequest"]["uploadUrl"]
                asset_urn = reg_data["value"]["asset"]

                # 3️⃣ Upload image to LinkedIn
                upload_headers = {"Authorization": f"Bearer {access_token}"}
                upload_resp = http.request(
                    "POST",
                    upload_url,
                    body=image_bytes,
                    headers=upload_headers
                )

                if upload_resp.status in [200, 201]:
                    media_assets.append({
                        "status": "READY",
                        "media": asset_urn
                    })
                    print(f"✅ Uploaded image {key} as {asset_urn}")
                else:
                    print(f"⚠️ Failed to upload image {key}: {upload_resp.status}")

            except Exception as e:
                print(f"⚠️ Error processing image {key}: {e}")

    # 4️⃣ Build LinkedIn post payload
    payload = {
        "author": f"urn:li:person:{sub}",
        "lifecycleState": "PUBLISHED",
        "specificContent": {
            "com.linkedin.ugc.ShareContent": {
                "shareCommentary": {"text": message},
                "shareMediaCategory": "IMAGE" if media_assets else "NONE",
                "media": media_assets if media_assets else []
            }
        },
        "visibility": {"com.linkedin.ugc.MemberNetworkVisibility": "PUBLIC"}
    }

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "X-Restli-Protocol-Version": "2.0.0"
    }

    # 5️⃣ Publish the post
    resp = http.request(
        "POST", "https://api.linkedin.com/v2/ugcPosts",
        body=json.dumps(payload), headers=headers
    )

    update_data = {
        "status_code": resp.status,
        "post_time": datetime.now(PKT).isoformat()
    }

    try:
        resp_data = json.loads(resp.data.decode())
        update_data["post_urn"] = resp_data.get("id")
        update_data["status"] = "posted" if resp.status == 201 else "failed"
    except Exception:
        update_data["status"] = "failed"

    # Update DynamoDB row
    post_table.update_item(
        Key={"sub": sub, "post_time": post_time.isoformat()},
        UpdateExpression="SET #s = :s, post_urn = :u, status_code = :c",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": update_data["status"],
            ":u": update_data.get("post_urn", "N/A"),
            ":c": update_data["status_code"]
        }
    )

    print(f"📌 Post for {sub} -> {update_data['status']}")
    return update_data


def _response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Headers": "Content-Type,Authorization"
        },
        "body": json.dumps(body)
    }