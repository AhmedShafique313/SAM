import json
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    bucket_name = 'cammi-devprod'


    try:
        # ✅ Extract session_id from first item (all items assumed to have the same one)
        session_id = event[0].get("session_id", None) 
        project_id = event[0].get("project_id", None)      

        # ✅ Extract session_id from first item (all items assumed to have the same one)
        user_id = event[0].get("user_id", None)  
        document_type = event[0].get("document_type", "")  

        if document_type:  # if not null or empty
            # object_key = f'flow/{document_type}/execution_plan.json'
            object_key = f'flow/{user_id}/{document_type}/execution_plan.json'
        else:
            object_key = 'flow/execution_plan.json'


        # 1. Validate all status are true
        if not all(item.get("status") is True for item in event):
            return {
                "message": "Not all items have status = true. No update performed.",
                "updated": False,
                "session_id": session_id,  # ✅ Add to response
                "user_id": user_id,  # ✅ Add to response
                "document_type": document_type,
                "project_id": project_id     
            }

        # 2. Extract tier name (assumes all have same tier)
        tier_name = event[0].get("tier")
        if not tier_name:
            return {
                "message": "Tier not found in event input.",
                "updated": False,
                "session_id": session_id,  # ✅ Add to response
                "user_id": user_id,  # ✅ Add to response  
                "document_type": document_type,
                "project_id": project_id                  
            }

        # 3. Load existing file from S3
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        data = json.loads(response['Body'].read())

        # 4. Update the tier status if it exists
        if tier_name in data:
            data[tier_name]['status'] = True
        else:
            return {
                "message": f"{tier_name} not found in the JSON file.",
                "updated": False,
                "session_id": session_id,  # ✅ Add to response
                "user_id": user_id,  # ✅ Add to response   
                "document_type": document_type,
                "project_id": project_id                 
            }

        # 5. Write updated data back to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json.dumps(data, indent=2),
            ContentType='application/json'
        )

        # 6. Check if any tier still has status = false
        any_incomplete = any(tier.get('status') is False for tier in data.values())

        return {
            "message": f"{tier_name} status updated to true.",
            "updated": True,
            "next_iteration": not any_incomplete,
            "session_id": session_id, 
            "user_id": user_id, 
            "document_type": document_type,
            "project_id": project_id            
        }

    except Exception as e:
        return {
            "error": str(e),
            "updated": False,
            "next_iteration": False,
            "session_id": session_id if 'session_id' in locals() else None,  # ✅ Safe fallback
            "user_id": user_id if 'user_id' in locals() else None  # ✅ Safe fallback
            
        }
