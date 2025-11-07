import json
import boto3

s3_client = boto3.client("s3")

def lambda_handler(event, context):
    # ✅ Extract values from input event
    session_id = event.get("session_id", "")
    user_id = event.get("user_id", "")
    project_id = event.get("project_id", "")
    document_type = event.get("document_type", "")

    # ✅ Define S3 bucket and construct paths
    bucket_name = "cammi-devprod"
    input_path = f"flow/{document_type}/execution_plan.json"
    output_path = f"flow/{user_id}/{document_type}/execution_plan.json"

    copy_status = "not_attempted"
    copy_message = ""

    # ✅ Copy file only if both user_id and document_type are provided
    if user_id and document_type:
        try:
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={"Bucket": bucket_name, "Key": input_path},
                Key=output_path
            )
            copy_status = "success"
            copy_message = f"File copied successfully to {output_path}"
        except Exception as e:
            copy_status = "error"
            copy_message = str(e)
    else:
        copy_message = "Missing user_id or document_type, copy skipped."

    # ✅ Return session info + S3 copy result
    return {
        "status": "success",
        "session_id": session_id,
        "project_id": project_id,
        "user_id": user_id,
        "document_type": document_type,
        "copy_status": copy_status,
        "copy_message": copy_message,
        "output_path": output_path if copy_status == "success" else None
    }
