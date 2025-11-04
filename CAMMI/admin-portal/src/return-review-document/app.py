import json
import boto3
import base64
import datetime
import os
from boto3.dynamodb.conditions import Key

# AWS clients
dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
# create SES client in configured region (or default if not set)
SES_REGION = os.getenv("SES_REGION", None)
ses = boto3.client("ses", region_name=SES_REGION) if SES_REGION else boto3.client("ses")

# Table
review_table = dynamodb.Table("review-document-table")

# Config
BUCKET_NAME = "cammi-devprod"
RETURNED_FOLDER = "ReturnedReviewDocuments"
SENDER_EMAIL = os.getenv("SENDER_EMAIL")  # REQUIRED: verified SES sender
PRESIGNED_EXPIRY = int(os.getenv("PRESIGNED_EXPIRY", "3600"))  # seconds

def build_response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
            "Access-Control-Allow-Headers": "Content-Type,Authorization",
        },
        "body": json.dumps(body)
    }

def create_presigned_url(bucket, key, expires=PRESIGNED_EXPIRY):
    try:
        return s3.generate_presigned_url(
            ClientMethod='get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expires
        )
    except Exception as e:
        print("Error generating presigned URL:", e)
        return None

def send_notification_email(recipient_email, project_name, document_type_uuid, feedback, download_url):
    if not SENDER_EMAIL:
        raise ValueError("SENDER_EMAIL environment variable is not set.")
    if not recipient_email:
        raise ValueError("Recipient email is missing.")

    subject = f"Your document review is complete — {project_name or 'Your Project'}"
    text_body = (
        f"Hello,\n\n"
        f"Your document ({document_type_uuid}) for project '{project_name or ''}' has been reviewed and marked as Done.\n\n"
        f"Feedback:\n{feedback or 'No feedback provided.'}\n\n"
        f"Download the reviewed document: {download_url or 'No file returned.'}\n\n"
        "If you have any questions, reply to this email.\n\n"
        "Regards,\nReview Team"
    )
    html_body = f"""
    <html>
      <body>
        <p>Hello,</p>
        <p>Your <strong>{document_type_uuid}</strong> document for project '<strong>{project_name or ""}</strong>' has been reviewed and marked as <strong>Done</strong>.</p>
        <p><strong>Feedback:</strong><br/>{(feedback or 'No feedback provided.')}</p>
        <p><a href="{download_url or '#'}">Click here to download the reviewed document</a></p>
        <p>Regards,<br/>Review Team</p>
      </body>
    </html>
    """

    resp = ses.send_email(
        Source=SENDER_EMAIL,
        Destination={"ToAddresses": [recipient_email]},
        Message={
            "Subject": {"Data": subject, "Charset": "UTF-8"},
            "Body": {
                "Text": {"Data": text_body, "Charset": "UTF-8"},
                "Html": {"Data": html_body, "Charset": "UTF-8"},
            },
        },
    )
    return resp  # contains MessageId on success

def lambda_handler(event, context):
    try:
        # CORS preflight
        if event.get("httpMethod") == "OPTIONS":
            return build_response(200, {"message": "CORS preflight OK"})

        body = json.loads(event["body"])

        project_id = body.get("project_id")
        document_type_uuid = body.get("document_type_uuid")
        feedback = body.get("feedback")
        document_text_base64 = body.get("document_text")

        if not project_id or not document_type_uuid:
            return build_response(400, {"error": "project_id and document_type_uuid are required."})

        # 1) Fetch existing ReviewDocument item
        resp = review_table.get_item(Key={"project_id": project_id, "document_type_uuid": document_type_uuid})
        item = resp.get("Item")
        if not item:
            return build_response(404, {"error": "Review document not found."})

        # store values for email
        recipient_email = item.get("email")
        project_name = item.get("project_name")
        original_s3_url = item.get("s3_url")

        # 2) Prepare update expressions for ReviewDocument
        update_expression = "SET #s = :s"
        expression_values = {":s": "done"}
        expression_names = {"#s": "status"}

        if feedback:
            update_expression += ", feedback = :f"
            expression_values[":f"] = feedback

        returned_s3_url = None
        presigned_url = None

        # 3) If document_text provided, upload to S3 and set returned_s3_url
        if document_text_base64:
            file_bytes = base64.b64decode(document_text_base64)
            s3_key = f"{RETURNED_FOLDER}/{project_id}/{document_type_uuid}.docx"
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=file_bytes,
                ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )
            returned_s3_url = f"s3://{BUCKET_NAME}/{s3_key}"
            update_expression += ", returned_s3_url = :r"
            expression_values[":r"] = returned_s3_url

            # create presigned for returned doc
            presigned_url = create_presigned_url(BUCKET_NAME, s3_key)

        else:
            # no returned doc; create presigned for original s3_url if available
            if original_s3_url and original_s3_url.startswith("s3://"):
                # parse bucket/key
                try:
                    _, rest = original_s3_url.split("s3://", 1)
                    bucket_in_url, key_in_url = rest.split("/", 1)
                    # If bucket matches BUCKET_NAME or not, use parsed values
                    presigned_url = create_presigned_url(bucket_in_url, key_in_url)
                except Exception as e:
                    print("Failed to parse original s3_url for presign:", e)
                    presigned_url = None

        # 4) Update the ReviewDocument entry in DynamoDB
        review_table.update_item(
            Key={"project_id": project_id, "document_type_uuid": document_type_uuid},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ExpressionAttributeNames=expression_names
        )

        # 5) Send email notification (if recipient available)
        email_sent = False
        email_response = None
        if recipient_email:
            try:
                email_response = send_notification_email(
                    recipient_email=recipient_email,
                    project_name=project_name,
                    document_type_uuid=document_type_uuid,
                    feedback=feedback,
                    download_url=presigned_url
                )
                email_sent = True
            except Exception as e:
                print("SES send error:", e)
                # Do not fail the whole lambda if email fails; just report it
                email_sent = False
                email_response = {"error": str(e)}

        return build_response(200, {
            "message": "Review document updated successfully.",
            "status": "done",
            "feedback_added": bool(feedback),
            "returned_document_uploaded": bool(document_text_base64),
            "returned_s3_url": returned_s3_url,
            "presigned_download_url": presigned_url,
            "email_sent": email_sent,
            "email_response": email_response
        })

    except Exception as e:
        return build_response(500, {"error": str(e)})
