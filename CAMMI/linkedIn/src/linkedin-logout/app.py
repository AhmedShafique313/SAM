import json

def lambda_handler(event, context):
    
    response = {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hello World from AWS Lambda!"
        })
    }
    
    return response