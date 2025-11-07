import json
import boto3

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'cammi-devprod'
    
    # Extract session_id from event
    try:
        session_id = event.get("session_id")
        project_id = event.get("project_id")
        user_id = event.get("user_id")
        document_type = event.get("document_type","")

        if document_type:  # if not null or empty
            # object_key = f'flow/{document_type}/execution_plan.json'
            object_key = f'flow/{user_id}/{document_type}/execution_plan.json'
        else:
            object_key = 'flow/execution_plan.json'
  
    except Exception:
        session_id = None
        user_id = None
        project_id = None
    try:
        # Read and load the JSON data from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        data = json.loads(response['Body'].read())

        # Loop through tiers in order
        for tier_key in sorted(data.keys(), key=lambda x: int(x.replace('tier', ''))):
            tier = data[tier_key]
            if isinstance(tier, dict) and tier.get('status') is False:
                # Build and return flat list
                result = [
                    {
                        "key": item["key"],
                        "status": False,
                        "tier": tier_key,
                        "session_id": session_id,
                        "project_id": project_id,
                        "user_id": user_id,
                        "document_type": document_type
                    }
                    for item in tier.get("items", [])
                ]
                return result  # ✅ Return flat list

        # All tiers passed
        return []  # ✅ Still return flat list

    except Exception as e:
        return {
            "error": str(e),
            "session_id": session_id,
            "project_id": project_id,
            "user_id": user_id,
            "document_type": document_type
        }
