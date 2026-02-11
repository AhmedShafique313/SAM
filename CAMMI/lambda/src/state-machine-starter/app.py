import boto3
import json
import uuid
import os
from datetime import datetime
from urllib.parse import urlparse
from botocore.exceptions import ClientError

# ---------- AWS Config ----------
REGION = os.environ.get("AWS_REGION", "us-east-1")
KNOWLEDGE_BASE_ID = os.environ.get("KNOWLEDGE_BASE_ID", "EPWIATHAOK")

# ---------- AWS Clients ----------
s3 = boto3.client("s3")
bedrock_runtime = boto3.client("bedrock-runtime", region_name=REGION)
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", region_name=REGION)
dynamodb = boto3.client("dynamodb")
dynamodb_resource = boto3.resource("dynamodb")

# ---------- Constants ----------
DDB_TABLE_NAME = "claude-usage-logs-table"

# ---------- Helpers ----------

def read_text_from_s3(s3_path: str):
    if not s3_path.startswith("s3://"):
        return False, "Invalid S3 path. Must start with 's3://'."
    
    s3_path = s3_path.replace("s3://", "")
    bucket, key = s3_path.split("/", 1)

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")
        return True, content
    except ClientError as e:
        return False, " "
    except Exception as e:
        return False, " "

def _read_s3_object(bucket: str, key: str) -> str:
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")

def _parse_s3_uri(uri: str):
    parsed = urlparse(uri)
    return parsed.netloc, parsed.path.lstrip("/")

def build_s3_path_from_key(key: str, variable: str, doc: str):
    parts = key.split("/")
    if len(parts) != 2:
        raise ValueError(f"Invalid key format. Expected format: 'folder/filename', got '{key}'")
    folder, filename = parts

    template_s3_uri = f"s3://cammi-devprod/{doc}/prompt/{key}/{filename}.txt"
    input_s3_uri = f"s3://cammi-devprod/{doc}/input/{key}/{filename}.txt"

    template_bucket, template_key = _parse_s3_uri(input_s3_uri)
    inputs = _read_s3_object(template_bucket, template_key)

    input_list = []
    for item in inputs.split(","):
        item = item.strip()
        if item.startswith("output/"):
            item = f"s3://cammi-devprod/{variable}/{doc}/{item}"
        elif item.startswith("prompt/"):
            item = f"s3://cammi-devprod/{doc}/{variable}/{item}"            
        input_list.append(item)

    input_list.insert(0, template_s3_uri)
    return input_list

def merge_using_template(template_text: str, replacements: dict) -> str:
    for key, value in replacements.items():
        template_text = template_text.replace(f"{{{{{key}}}}}", value)
    return template_text

def build_prompt(s3_uris: list, context_text: str = "") -> str:
    template_bucket, template_key = _parse_s3_uri(s3_uris[0])
    template = _read_s3_object(template_bucket, template_key)

    replacements = {}
    for uri in s3_uris[1:]:
        b, k = _parse_s3_uri(uri)
        try:
            content = _read_s3_object(b, k)
            key = k.split("/")[-1].split(".")[0]
            replacements[key] = content
        except s3.exceptions.NoSuchKey:
            print(f"[ SKIPPED] S3 path not found: s3://{b}/{k}")
        except Exception as e:
            print(f"[ ERROR] Could not read s3://{b}/{k} — {str(e)}")

    base_prompt = merge_using_template(template, replacements)
    
    if context_text:
        base_prompt = f"Context from Knowledge Base:\n{context_text}\n\n{base_prompt}"
    
    return base_prompt

# ---------- DynamoDB Logic ----------
def ensure_ddb_table_exists():
    tables = dynamodb.list_tables()['TableNames']
    if DDB_TABLE_NAME not in tables:
        table = dynamodb_resource.create_table(
            TableName=DDB_TABLE_NAME,
            KeySchema=[
                {'AttributeName': 'session_id', 'KeyType': 'HASH'},
                {'AttributeName': 'run_id', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'session_id', 'AttributeType': 'S'},
                {'AttributeName': 'run_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        table.wait_until_exists()

def log_to_dynamodb(session_id, input_tokens, output_tokens, cost_usd):
    ensure_ddb_table_exists()
    run_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat()

    try:
        dynamodb.put_item(
            TableName=DDB_TABLE_NAME,
            Item={
                'session_id': {'S': session_id},
                'run_id': {'S': run_id},
                'timestamp': {'S': timestamp},
                'input_tokens': {'N': str(input_tokens)},
                'output_tokens': {'N': str(output_tokens)},
                'estimated_cost_usd': {'N': str(cost_usd)}
            }
        )
    except ClientError as e:
        print(f"[DynamoDB Error] {e.response['Error']['Message']}")

# ---------- Knowledge Base Retrieval ----------
def retrieve_with_filter(user_id: str, query: str, max_results: int = 5):
    return bedrock_agent_runtime.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={
            "vectorSearchConfiguration": {
                "numberOfResults": max_results,
                "filter": {"equals": {"key": "user_id", "value": user_id}}
            }
        }
    )

def retrieve_without_filter(query: str, max_results: int = 5):
    return bedrock_agent_runtime.retrieve(
        knowledgeBaseId=KNOWLEDGE_BASE_ID,
        retrievalQuery={"text": query},
        retrievalConfiguration={"vectorSearchConfiguration": {"numberOfResults": max_results}}
    )

def extract_chunks_and_log_metadata(response: dict):
    results = response.get("retrievalResults", [])
    for r in results:
        print("KB METADATA FOUND:", json.dumps(r.get("metadata", {})))
    return [r["content"]["text"] for r in results if r.get("content", {}).get("text")]

# ---------- Main Bedrock Function ----------
def bedrock_from_s3_files_converse(result: str, doc: str, key: str, s3_uris: list[str], model_id: str, max_tokens: int, temperature: float, session_id: str, project_id: str, user_id: str, additional_prompt: str = "") -> dict:
    
    retrieval_prompt = (
        "Generate execution-ready social media campaign strategy including "
        "content ideas, messaging, platform plan, CTA, and posting schedule."
    )
    
    filtered_response = retrieve_with_filter(user_id, retrieval_prompt)
    chunks = extract_chunks_and_log_metadata(filtered_response)
    
    if not chunks:
        print("Filtered retrieval empty. Falling back.")
        unfiltered_response = retrieve_without_filter(retrieval_prompt)
        chunks = extract_chunks_and_log_metadata(unfiltered_response)
    
    context_text = "\n\n".join(chunks)

    base_prompt = build_prompt(s3_uris, context_text=context_text)

    formatting_instruction = f"""
OUTPUT FORMATTING RULES (CRITICAL):
- Write in PLAIN TEXT ONLY
- Do NOT use markdown formatting (no #, ##, ###, *, -, >, or any markdown symbols)
- Use paragraph breaks and line spacing for structure
- Use simple text labels with colons (e.g., "Section Name:") instead of headers
- Write in continuous prose with clear paragraph separation
- No bold, italics, bullet points, or numbered lists using markdown syntax
This is the contextual information for reference and for user tailored experience: 
{result}
"""

    EXCLUDED_DOC_TYPES = ["qmp", "cc"]
    if doc not in EXCLUDED_DOC_TYPES:
        base_prompt = f"{formatting_instruction}{base_prompt.strip()}"

    if additional_prompt:
        prompt = f"""{base_prompt.strip()}

Role: You are a professional business writer skilled in transforming vague client feedback into clear, strategic, and polished content.
Goal: Refine the selected section to better reflect the client’s expectations while maintaining alignment with the original brief and format.
Task Description:  
Below is client feedback on the previous response generated: {additional_prompt.strip()}.
Ensure the new version is targeted, and aligned with the original input above and instructions."""
    else:
        prompt = base_prompt.strip()

    conversation = [{"role": "user", "content": [{"text": prompt}]}]

    try:
        response = bedrock_runtime.converse(
            modelId=model_id,
            messages=conversation,
            inferenceConfig={"maxTokens": max_tokens, "temperature": temperature, "topP": 0.9},
            requestMetadata={"sessionId": session_id}
        )

        response_text = response["output"]["message"]["content"][0]["text"]
        usage = response.get("usage", {})
        input_tokens = usage.get("inputTokens", 0)
        output_tokens = usage.get("outputTokens", 0)
        total_cost = round(input_tokens * 0.000003 + output_tokens * 0.000015, 6)

        folder, filename = key.split("/")
        output_bucket = "cammi-devprod"
        output_key = f"{project_id}/{doc}/output/{key}/{filename}.txt"
        s3.put_object(Bucket=output_bucket, Key=output_key, Body=response_text.encode("utf-8"))

        log_to_dynamodb(session_id, input_tokens, output_tokens, total_cost)

        return {
            "session_id": session_id,
            "token_usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
            "estimated_cost_usd": total_cost
        }

    except (ClientError, Exception) as e:
        return {"error": f"Error invoking model: {str(e)}", "session_id": session_id}

# ---------- Lambda Handler ----------
def lambda_handler(event, context):
    try:
        # Handle Payload-wrapped events (e.g., from Step Functions)
        if "Payload" in event:
            event = event["Payload"]

        model_id = event.get("model_id", "us.anthropic.claude-sonnet-4-20250514-v1:0")
        max_tokens = event.get("maxTokens", 1000)
        temperature = event.get("temperature", 0.5)
        key = event.get("key", "")
        session_id = event.get("session_id", "")
        project_id = event.get("project_id", "")
        user_id = event.get("user_id", "")
        document_type = event.get("document_type", "")

        if len(key.split("/")) != 2:
            raise ValueError(f"Invalid key format. Expected format: 'folder/filename', got '{key}'")

        url_parsing = f"s3://cammi-devprod/url_parsing/{project_id}/{user_id}/web_scraping.txt"
        success, result = read_text_from_s3(url_parsing)

        flag = event.get("edit_flag", False)
        additional_prompt = event.get("prompt", "") if flag else ""

        path = build_s3_path_from_key(key, project_id, document_type)

        result = bedrock_from_s3_files_converse(
            result=result,
            doc=document_type,
            key=key,
            s3_uris=path,
            model_id=model_id,
            max_tokens=max_tokens,
            temperature=temperature,
            session_id=session_id,
            project_id=project_id,
            user_id=user_id,
            additional_prompt=additional_prompt
        )

        event.update({
            "status": True,
            "session_id": session_id,
            "project_id": project_id,
            "user_id": user_id,
            "document_type": document_type,
            "result": result
        })

        return event

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e),
                "session_id": event.get("session_id", ""),
                "project_id": event.get("project_id", ""),
                "user_id": event.get("user_id", ""),
                "document_type": event.get("document_type", "")
            })
        }
