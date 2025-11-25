import json
import boto3

# Initialize AWS clients
s3 = boto3.client('s3')
stepfunctions = boto3.client('stepfunctions')
dynamodb = boto3.client('dynamodb')

# Replace with your Step Function ARN
STATE_MACHINE_ARN = 'arn:aws:states:us-east-1:687088702813:stateMachine:unified-state-machine'

# Replace with your DynamoDB table name
USERS_TABLE_NAME = 'users-table'
SESSION_ID_INDEX = 'session_id-index'  # Change if your GSI name is different

def lambda_handler(event, context):
    try:
        # Log the event for debugging
        print("Event:", json.dumps(event))

        # Extract bucket and key from S3 event
        record = event['Records'][0]
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # ✅ Retrieve metadata from S3 object
        obj_head = s3.head_object(Bucket=bucket, Key=key)
        
        # Retrieve "token" as session_id from metadata
        session_id = obj_head['Metadata'].get('token', None)

        # Retrieve "project_id" from metadata
        project_id = obj_head['Metadata'].get('project_id', None)  
        document_type = obj_head['Metadata'].get('document_type', None) 
        if not project_id:
            print("Warning: project_id not found in S3 metadata")
            project_id = "unknown"              

        if not session_id:
            print("Warning: session_id (token) not found in S3 metadata")
            session_id = "unknown"

        # ✅ Get "id" from DynamoDB using session_id via GSI
        user_id = None
        if session_id != "unknown":
            try:
                response = dynamodb.query(
                    TableName=USERS_TABLE_NAME,
                    IndexName=SESSION_ID_INDEX,
                    KeyConditionExpression="session_id = :sid",
                    ExpressionAttributeValues={":sid": {"S": session_id}}
                )
                if response.get("Items"):
                    user_id = response["Items"][0]["id"]["S"]
                    print(f"Found id: {user_id} for session_id: {session_id}")
                else:
                    print(f"No record found for session_id: {session_id}")
            except Exception as db_err:
                print(f"Error fetching id from DynamoDB: {str(db_err)}")

        # Prepare input for Step Function
        step_input = {
            "bucket": bucket,
            "key": key,
            "session_id": session_id,
            "user_id": user_id,
            "project_id":project_id,
            "document_type": document_type
        }

        # Start Step Function execution
        response = stepfunctions.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            input=json.dumps(step_input)
        )

        print("Step Function started:", response['executionArn'])
        return {
            'statusCode': 200,
            'body': json.dumps('Step Function execution started successfully.'),
            'event_input': step_input
        }

    except Exception as e:
        print("Error starting Step Function:", str(e))
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
