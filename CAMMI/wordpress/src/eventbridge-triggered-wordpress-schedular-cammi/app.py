# poster_runner.py
from __future__ import annotations
import json, os, mimetypes, boto3, uuid, base64, re
from datetime import datetime, timezone
from urllib.parse import urlparse
import urllib3
 
# -----------------------
# AWS Clients
# -----------------------
dynamodb = boto3.resource("dynamodb")
posts_table = dynamodb.Table("wordpress-posts-table")
sites_table = dynamodb.Table("wordpress-sites-table")
s3 = boto3.client("s3")
 
S3_BUCKET = "cammi-devprod"
http = urllib3.PoolManager()
 
# -----------------------
# Helpers
# -----------------------
def secure_filename(filename: str) -> str:
    """Simple replacement for werkzeug's secure_filename"""
    filename = os.path.basename(filename)
    filename = re.sub(r"[^A-Za-z0-9_.-]", "_", filename)
    return filename or "file"
 
def rest_base(base_url: str) -> str:
    return base_url.rstrip("/") + "/wp-json/"
 
def guess_mime(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"
 
def upload_media_to_wp(site: dict, image_bytes: bytes, filename: str) -> dict:
    base = rest_base(site["base_url"])
    url = base + "wp/v2/media"
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            f"{site['username']}:{site['app_password']}".encode()
        ).decode(),
        "Content-Disposition": f'attachment; filename="{secure_filename(filename)}"',
        "Content-Type": guess_mime(filename)
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
 
def create_post_on_wp(site: dict, title: str, content: str, featured_media_id: int | None, publish_at_utc: datetime | None) -> dict:
    base = rest_base(site["base_url"])
    url = base + "wp/v2/posts"
    payload = {"title": title, "status": "publish"}
    if content:
        payload["content"] = content
    if featured_media_id:
        payload["featured_media"] = int(featured_media_id)
    # If future publish (already handled by scheduler), we can skip date_gmt
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            f"{site['username']}:{site['app_password']}".encode()
        ).decode(),
        "Content-Type": "application/json"
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
# Lambda Handler
# -----------------------
def lambda_handler(event, context):
    # Extract post_id and publish_at from EventBridge
    detail = event.get("detail") or {}
    if "input" in detail and isinstance(detail["input"], str):
        payload = json.loads(detail["input"])
        post_id = payload.get("post_id")
        publish_at = payload.get("publish_at")
    else:
        post_id = event.get("post_id") or detail.get("post_id")
        publish_at = event.get("publish_at") or detail.get("publish_at")
 
    if not post_id or not publish_at:
        raise ValueError("Event must contain post_id and publish_at")
 
    # Fetch post record from DynamoDB
    resp = posts_table.get_item(
        Key={
            "post_id": post_id,
            "publish_at": publish_at
        }
    )
    post = resp.get("Item")
    if not post:
        raise ValueError(f"Post {post_id} with publish_at {publish_at} not found")
 
    # Skip if already published
    if post.get("status") == "publish":
        return {"status": "already_published", "post_id": post_id}
 
    # Fetch site credentials
    sitename = post.get("sitename")
    site_resp = sites_table.get_item(Key={"sitename": sitename})
    site = site_resp.get("Item")
    if not site:
        raise ValueError(f"Site {sitename} not found")
 
    # Upload images from S3
    image_keys = post.get("image_keys", []) or []
    featured_media_id = None
    media_src_url = None
    for idx, key in enumerate(image_keys):
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        data = obj["Body"].read()
        filename = key.split("/")[-1]
        media = upload_media_to_wp(site, data, filename)
        if idx == 0:
            featured_media_id = media.get("id")
            media_src_url = media.get("source_url")
 
    # Prepare content
    content_html = post.get("content_html", "")
    if post.get("embed") and media_src_url:
        content_html = f'<figure><img src="{media_src_url}" alt="" /></figure>\n' + content_html
 
    # Publish immediately (scheduler already invoked at correct time)
    wp_post = create_post_on_wp(site, post.get("title"), content_html, featured_media_id, None)
 
    # Update DynamoDB
    posts_table.update_item(
        Key={
            "post_id": post_id,
            "publish_at": publish_at
        },
        UpdateExpression="SET #s = :s, wp_id = :w, link = :l, date_gmt = :d, updated_at = :u",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":s": wp_post.get("status"),
            ":w": str(wp_post.get("id")),
            ":l": wp_post.get("link"),
            ":d": wp_post.get("date_gmt"),
            ":u": datetime.now(timezone.utc).isoformat()
        }
    )
 
    return {"status": "posted", "post_id": post_id, "link": wp_post.get("link")}
 
 