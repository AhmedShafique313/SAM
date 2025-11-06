from __future__ import annotations
import json, os, mimetypes, boto3, uuid, base64, re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from urllib.parse import urlparse
import urllib3
 
# -----------------------
# Hardcoded Configs
# -----------------------
# S3_BUCKET = "wordpress-data-cammi"
# POSTER_LAMBDA_ARN = "arn:aws:lambda:us-east-1:166072979225:function:wordpress_schedular_cammi2"
# SCHEDULER_ROLE_ARN = "arn:aws:iam::166072979225:role/WordPress_Schedular_Policy"


S3_BUCKET = "wordpress-data-schedule-cammi"
POSTER_LAMBDA_ARN = "arn:aws:lambda:us-east-1:468943998235:function:wordpress_schedular_cammi2"
SCHEDULER_ROLE_ARN = "arn:aws:iam::468943998235:role/WordPress_Schedular_Policy"
 
# AWS Resources
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
scheduler = boto3.client("scheduler")
 
sites_table = dynamodb.Table("wordpress-sites-table")
posts_table = dynamodb.Table("wordpress-posts-table")
 
http = urllib3.PoolManager()
 
# -----------------------
# Helpers
# -----------------------
def secure_filename(filename: str) -> str:
    """Minimal replacement for werkzeug's secure_filename"""
    filename = os.path.basename(filename)
    filename = re.sub(r"[^A-Za-z0-9_.-]", "_", filename)
    return filename or "file"
 
def rest_base(base_url: str) -> str:
    return base_url.rstrip("/") + "/wp-json/"
 
def guess_mime(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"
 
def upload_media(site: dict, image_bytes: bytes, filename: str) -> dict:
    base = rest_base(site["base_url"])
    url = base + "wp/v2/media"
 
    headers = {
        "Content-Disposition": f'attachment; filename="{secure_filename(filename)}"',
        "Authorization": "Basic " + base64.b64encode(
            f"{site['username']}:{site['app_password']}".encode()
        ).decode(),
        "Content-Type": guess_mime(filename),
    }
 
    resp = http.request(
        "POST",
        url,
        body=image_bytes,
        headers=headers
    )
    if resp.status >= 400:
        raise Exception(f"Upload failed: {resp.status} {resp.data}")
    return json.loads(resp.data.decode())
 
def create_post(site: dict, title: str, content: str | None,
                featured_media: int | None, publish_at_utc: datetime | None) -> dict:
    base = rest_base(site["base_url"])
    url = base + "wp/v2/posts"
 
    payload = {"title": title, "status": "publish"}
    if content:
        payload["content"] = content
    if featured_media:
        payload["featured_media"] = int(featured_media)
 
    now_utc = datetime.now(timezone.utc)
    if publish_at_utc and publish_at_utc > now_utc:
        payload["status"] = "future"
        payload["date_gmt"] = publish_at_utc.strftime("%Y-%m-%dT%H:%M:%S")
 
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            f"{site['username']}:{site['app_password']}".encode()
        ).decode(),
        "Content-Type": "application/json",
    }
 
    resp = http.request(
        "POST",
        url,
        body=json.dumps(payload),
        headers=headers
    )
    if resp.status >= 400:
        raise Exception(f"Post creation failed: {resp.status} {resp.data}")
    return json.loads(resp.data.decode())
 
# -----------------------
# Scheduler Helper
# -----------------------
def schedule_post(post_id: str, run_at_utc: datetime):
    schedule_name = f"WordPressScheduler_{post_id}"
    scheduler.create_schedule(
        Name=schedule_name,
        ScheduleExpression=f"at({run_at_utc.strftime('%Y-%m-%dT%H:%M:%S')})",
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": POSTER_LAMBDA_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"post_id": post_id}),
        },
    )
    return schedule_name
 
# -----------------------
# Lambda Handler
# -----------------------
def lambda_handler(event, context):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Allow-Methods": "OPTIONS,POST"
    }
 
    body = json.loads(event["body"]) if "body" in event else event
 
    sitename = body.get("sitename")
    title = body.get("title")
    content_html = body.get("content_html", "")
    image_url = body.get("image_url")
    embed = body.get("embed", False)
    publish_at = body.get("publish_at")  # "2025-09-10T15:00:00"
 
    if not sitename or not title:
        return {
            "statusCode": 400,
            "headers": headers,
            "body": json.dumps({"error": "sitename and title required"})
        }
 
    # Fetch site credentials
    resp = sites_table.get_item(Key={"sitename": sitename})
    site = resp.get("Item")
    if not site:
        return {
            "statusCode": 404,
            "headers": headers,
            "body": json.dumps({"error": "site not found"})
        }
 
    # Handle publish time (PKT -> UTC)
    publish_at_utc = None
    if publish_at:
        local_zone = ZoneInfo("Asia/Karachi")
        dt = datetime.fromisoformat(publish_at)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=local_zone)
        else:
            dt = dt.astimezone(local_zone)
        publish_at_utc = dt.astimezone(timezone.utc)
    else:
        # fallback to current UTC time
        publish_at = datetime.now(timezone.utc).isoformat()
 
    # Upload media (to S3 + WP if immediate)
    featured_media_id = None
    media_src_url = None
    image_keys = []
 
    if image_url:
        r = http.request("GET", image_url)
        if r.status >= 400:
            raise Exception(f"Image download failed: {r.status}")
        filename = os.path.basename(urlparse(image_url).path) or "image"
 
        # Save to S3 bucket
        s3_key = f"{uuid.uuid4()}_{secure_filename(filename)}"
        s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=r.data)
        image_keys.append(s3_key)
 
        # If immediate post, upload to WordPress right away
        if not publish_at_utc:
            media = upload_media(site, r.data, filename)
            featured_media_id = media.get("id")
            media_src_url = media.get("source_url")
 
    final_content = content_html
    if embed and media_src_url:
        final_content = f'<figure><img src="{media_src_url}" alt="" /></figure>\n' + final_content
 
    # Prepare post record
    post_id = str(uuid.uuid4())
    post_item = {
        "post_id": post_id,
        "sitename": sitename,
        "title": title,
        "status": "scheduled" if publish_at_utc else "published",
        "publish_at": publish_at,   # ✅ always set (sort key)
        "timezone": "Asia/Karachi",
        "image_keys": image_keys,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
 
    if not publish_at_utc:
        # Immediate post to WordPress
        wp_post = create_post(site, title, final_content, featured_media_id, publish_at_utc)
        post_item.update({
            "wp_id": str(wp_post.get("id")),
            "status": wp_post.get("status"),
            "link": wp_post.get("link"),
            "date_gmt": wp_post.get("date_gmt"),
        })
    else:
        # Schedule via EventBridge
        schedule_name = schedule_post(post_id, publish_at_utc)
        post_item["schedule_name"] = schedule_name
 
    # Save post info
    posts_table.put_item(Item=post_item)
 
    return {
        "statusCode": 201,
        "headers": headers,
        "body": json.dumps({
            "message": "✅ Post created successfully!",
            "post": post_item
        })
    }
