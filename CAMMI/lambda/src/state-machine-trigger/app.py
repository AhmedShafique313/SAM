import json

def lambda_handler(event, context):
    # Simple Hello World response
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Hello World from Lambda!"
        })
    }