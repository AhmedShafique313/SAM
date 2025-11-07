import json
 
def lambda_handler(event, context):
    try:
        # Extract the body string
        body = event.get("body", "[]")
 
        # Parse the body string into a Python list of dicts
        parsed_body = json.loads(body)
 
        # Directly return the parsed dictionary (Step Function will use this as input for the next Lambda)
        return parsed_body
 
    except Exception as e:
        return {
            "error": str(e)
        }