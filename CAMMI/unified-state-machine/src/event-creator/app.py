import boto3

import json

from boto3.dynamodb.conditions import Attr
 
# DynamoDB client

dynamodb = boto3.resource("dynamodb")

users_table = dynamodb.Table("users-table")  # change to your actual table name
 
def lambda_handler(event, context):

    """

    Step Function triggers this Lambda.

    event is a list of results, each containing session_id, project_id, user_id, etc.

    This Lambda transforms the event into a WebSocket-style event

    and passes it to the next Lambda in the State Machine.

    """

    print("Incoming event:", json.dumps(event))
 
    if not isinstance(event, list) or len(event) == 0:

        return {"statusCode": 400, "body": "Expected a non-empty list"}
 
    # All items share the same session_id → pick from the first one

    session_id = event[0].get("session_id")

    if not session_id:

        return {"statusCode": 400, "body": "session_id missing in input"}
 
    # Get connectionId from DynamoDB

    response = users_table.scan(

        FilterExpression=Attr("session_id").eq(session_id)

    )

    items = response.get("Items", [])

    if not items:

        return {"statusCode": 404, "body": f"No user found with session_id {session_id}"}
 
    connection_id = items[0]["connection_id"]
 
    # ✅ Format the output event for the next Lambda

    next_event = {

        "requestContext": {

            "routeKey": "sendMessage",

            "connectionId": connection_id,

            "eventType": "MESSAGE",

            "domainName": "4iqvtvmxle.execute-api.us-east-1.amazonaws.com",

            "stage": "prod"

        },

        # Put the FULL array inside body (stringified)

        "body": json.dumps(event)

    }
 
    print("Transformed event for next Lambda:", json.dumps(next_event))

    return next_event

 