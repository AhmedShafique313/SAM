# -*- coding: utf-8 -*-
"""
AWS Lambda: Build marketing DOCX from S3 template + content
- Auto-detect GitHub-style Markdown tables (no TABLE_START/TABLE_END needed)
- Convert Markdown tables -> real DOCX tables (Light Grid Accent 1)
- Keep headings, bullets, and Label:Value formatting
"""
import uuid 
import boto3
import json
from io import BytesIO
from botocore.exceptions import ClientError
from docx import Document
from docx.shared import Pt, RGBColor
from docx.oxml.ns import qn
from datetime import datetime
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
import re
import difflib
import base64
from docx.shared import Inches


# ---------- AWS ----------
s3 = boto3.client('s3')
# DynamoDB resource (initialize globally)
dynamodb = boto3.resource('dynamodb')
DOCUMENT_HISTORY_TABLE = "documents-history-table"

# ---------- CONFIG ----------
TABLE_STYLE = "Light Grid Accent 1"
DOCUMENT_TYPE_NAMES = {
    "gtm": "Go to Market",
    "icp": "Ideal Customer Profile",
    "kmf": "Key Messaging Framework",
    "bs": "Brand Strategy",
    "sr": "Strategy Roadmap",
    "mr": "Market Research",
    "brand": "Brand",
    "messaging": "Messaging",
    "smp": "Strategic Marketing Plan"
}


def save_document_history_to_dynamodb(user_id, project_id, document_type, document_name, document_url):
    """
    Inserts a new record into the DocumentHistory DynamoDB table.

    document_type_uuid = "{document_type}#<UUID>"
    """
    try:
        table = dynamodb.Table(DOCUMENT_HISTORY_TABLE)

        document_type_uuid = f"{document_type}#{uuid.uuid4()}"
        created_at = datetime.utcnow().isoformat()

        item = {
            "user_id": user_id,
            "document_type_uuid": document_type_uuid,
            "project_id": project_id,
            "document_type": document_type,
            "document_name": document_name,
            "document_url": document_url,
            "created_at": created_at
        }

        table.put_item(Item=item)

        print(f"✅ Document history saved: {document_type_uuid}")
        return True

    except ClientError as e:
        print(f"❌ DynamoDB error while saving document history: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error while saving document history: {e}")
        return False

# ---------- Helpers: formatting ----------
def format_heading(text: str) -> str:
    return ' '.join(word.capitalize() for word in text.replace("_", " ").split())

# ---------- Text cleanup ----------
def clean_text(text: str) -> str:
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # don't collapse single newlines (tables depend on line structure)
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    return text.strip()

def unbreak_paragraphs(text: str) -> str:
    """
    Groups soft-wrapped lines into paragraphs — BUT this should NOT
    run on blocks that contain Markdown tables (we guard for that upstream).
    """
    lines = text.splitlines()
    result = []
    buffer = ""

    for line in lines:
        stripped = line.strip()

        if not stripped:
            if buffer:
                result.append(buffer.strip())
                buffer = ""
            continue

        # bullets are separate
        if stripped.startswith(("-", "*", "•")):
            if buffer:
                result.append(buffer.strip())
                buffer = ""
            result.append(stripped)
            continue

        # "Label: Value" lines are separate
        if ':' in stripped and not stripped.endswith(':'):
            if buffer:
                result.append(buffer.strip())
                buffer = ""
            result.append(stripped)
            continue

        # normal line join
        if buffer and not buffer.endswith(('.', ':', '?', '!', '"', '"')):
            buffer += " " + stripped
        else:
            buffer += ("\n" if buffer else "") + stripped

    if buffer:
        result.append(buffer.strip())

    return "\n\n".join(result)

# ---------- S3 I/O ----------
def load_template_from_s3(bucket, key):
    """
    Load a DOCX template from S3 and return a Document object.
    """
    response = s3.get_object(Bucket=bucket, Key=key)
    template_stream = BytesIO(response['Body'].read())
    return Document(template_stream)

def read_json_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def read_text_file_from_s3(s3_path):
    """
    Reads UTF-8 text. If it contains a Markdown table, we DO NOT run
    unbreak_paragraphs() (to preserve row lines). Otherwise, we do.
    """
    try:
        s3_path = s3_path.replace("s3://", "")
        bucket, key = s3_path.split("/", 1)

        response = s3.get_object(Bucket=bucket, Key=key)
        raw_text = response['Body'].read().decode('utf-8').strip()
        if not raw_text:
            return "[Content missing]"

        cleaned = clean_text(raw_text)

        # Detect GitHub-style Markdown table: header + separator row like |---|
        lines = cleaned.split("\n")
        has_md_table = False
        sep_re = re.compile(r'^\s*\|?\s*:?-{3,}\s*(\|\s*:?-{3,}\s*)+\|?\s*$')

        for i in range(len(lines) - 1):
            if "|" in lines[i] and sep_re.match(lines[i + 1] or ""):
                has_md_table = True
                break

        # Only unbreak when there's no table (tables rely on exact line structure)
        if has_md_table:
            return cleaned
        else:
            return unbreak_paragraphs(cleaned)

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return "[Content missing]"
        raise e

# ---------- Exact heading matching (skip duplicate H1s inside content) ----------
def _normalize_heading(s: str) -> str:
    s = re.sub(r'\s+', ' ', s).strip().lower()
    s = re.sub(r'[^a-z0-9 ]+', '', s)
    return s

def exact_match(a: str, b: str) -> bool:
    """
    Exact matching for headings - only skip if they're identical
    after normalization (spaces, case, punctuation removed)
    """
    na, nb = _normalize_heading(a), _normalize_heading(b)
    return na == nb

# ---------- Markdown table -> real DOCX table ----------
def markdown_table_to_docx(md_table: str, doc: Document):
    lines = [line.strip() for line in md_table.strip().split("\n") if line.strip()]
    if len(lines) < 2:
        return

    # First non-empty line is header; skip the next separator row(s)
    header = [h.strip() for h in lines[0].split("|") if h.strip()]
    if not header:
        return

    # Build rows (skip separator lines)
    rows = []
    sep_re = re.compile(r'^\s*\|?\s*:?-{3,}\s*(\|\s*:?-{3,}\s*)+\|?\s*$')
    for line in lines[1:]:
        if sep_re.match(line):
            continue
        if "|" in line:
            row = [c.strip() for c in line.split("|") if c.strip()]
            if row:
                rows.append(row)

    table = doc.add_table(rows=len(rows) + 1, cols=len(header))
    try:
        table.style = TABLE_STYLE
    except Exception:
        pass
    table.autofit = True

    # header
    for j, h in enumerate(header):
        cell = table.cell(0, j)
        cell.text = h
        for paragraph in cell.paragraphs:
            for run in paragraph.runs:
                run.bold = True

    # body
    for i, row in enumerate(rows):
        for j, txt in enumerate(row):
            cell = table.cell(i + 1, j)
            cell.text = txt

# ---------- Content renderer ----------
def add_formatted_paragraphs(document: Document, text: str, template_h1: str = ""):
    """
    - Auto-detects GitHub-style Markdown tables (header + --- separator)
    - Converts them into real DOCX tables
    - Recognizes markdown headings (#, ##, ###)
    - Skips H1 if exact-matching template_h1 (avoid dupes)
    - Bullets: -, *, •
    - "Label:" or "Label: Value" → bold label
    """
    text = clean_text(text)
    lines = text.split("\n")

    i = 0
    sep_re = re.compile(r'^\s*\|?\s*:?-{3,}\s*(\|\s*:?-{3,}\s*)+\|?\s*$')

    def flush_table_from(start_idx):
        """
        Given index of header row, consume:
          header line
          one or more separator lines
          subsequent lines with pipes (rows)
        Return (end_idx_exclusive, md_table_string)
        """
        header_line = lines[start_idx].strip()
        j = start_idx + 1

        # must have at least one separator line
        if j >= len(lines) or not sep_re.match(lines[j].strip()):
            return start_idx, None

        buf = [header_line]
        # include all consecutive separator lines (rare but allowed)
        while j < len(lines) and sep_re.match(lines[j].strip()):
            buf.append(lines[j].strip())
            j += 1

        # include table rows: lines that contain at least one pipe
        while j < len(lines) and '|' in lines[j]:
            buf.append(lines[j].strip())
            j += 1

        return j, "\n".join(buf)

    while i < len(lines):
        line = lines[i].strip()

        # try table detection first (header + separator)
        if "|" in line and (i + 1) < len(lines) and sep_re.match(lines[i + 1].strip()):
            end_idx, md_table = flush_table_from(i)
            if md_table:
                markdown_table_to_docx(md_table, document)
                document.add_paragraph()
                i = end_idx
                continue  # next line after table

        # blank line -> spacing
        if not line:
            document.add_paragraph()
            i += 1
            continue

        # H3
        if line.startswith("### "):
            document.add_paragraph(line[4:].strip(), style="Heading 3")
            i += 1
            continue

        # H2
        if line.startswith("## "):
            document.add_paragraph(line[3:].strip(), style="Heading 2")
            i += 1
            continue

        # H1 (skip if identical to template heading)
        if line.startswith("# "):
            h1_text = line[2:].strip()
            if not (template_h1 and exact_match(h1_text, template_h1)):
                document.add_paragraph(h1_text, style="Heading 1")
            i += 1
            continue

        # bullets
        if line.startswith(("-", "*", "•")):
            content = line[1:].strip()
            try:
                p = document.add_paragraph(style='List Paragraph')
            except KeyError:
                p = document.add_paragraph()
                p.style = document.styles['Normal']
                p.paragraph_format.left_indent = Pt(18)
            if ':' in content:
                # Handle both "Label:" and "Label: Value" formats
                if content.endswith(':'):
                    # Case: "Revenue Streams:" - make entire content bold
                    run = p.add_run(content)
                    run.bold = True
                else:
                    # Case: "Label: Value" - make only label part bold
                    label, _, rest = content.partition(':')
                    run_b = p.add_run(label.strip() + ": ")
                    run_b.bold = True
                    run_n = p.add_run(rest.strip())
            else:
                p.add_run(content)
            i += 1
            continue

        # Label: Value OR Label: (standalone labels ending with colon)
        if ':' in line:
            if line.endswith(':'):
                # Case: "Revenue Streams:" - entire line bold
                p = document.add_paragraph()
                run = p.add_run(line)
                run.bold = True
            else:
                # Case: "Label: Value" - only label part bold
                label, value = line.split(':', 1)   # split only once
                label = label.strip()
                value = value.strip()

                p = document.add_paragraph()

                # Bold label
                run_b = p.add_run(label + ": ")
                run_b.bold = True

                # Normal value (explicitly un-bolded)
                if value:
                    run_n = p.add_run(value)
                    run_n.bold = False

            i += 1
            continue        

        # default paragraph
        document.add_paragraph(line)
        i += 1

# ---------- Status updater ----------
def update_status_to_false(bucket_name, object_key):
    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)

        for tier in data.values():
            if isinstance(tier, dict) and 'status' in tier:
                tier['status'] = False

        updated_content = json.dumps(data, indent=2)
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=updated_content.encode('utf-8'))

        print("✅ Status keys updated successfully.")

    except ClientError as e:
        print(f"❌ AWS Error: {e}")
    except json.JSONDecodeError as e:
        print(f"❌ JSON Error: {e}")
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")


def save_project_logo_to_s3(project_id: str, logo_base64: str):
    """
    Saves project logo to:
    s3://cammi-devprod/logos/{project_id}/logo.png
    Overwrites if already exists.
    """
    try:
        if not logo_base64:
            return None

        # Remove data:image/...;base64,
        if "," in logo_base64:
            logo_base64 = logo_base64.split(",", 1)[1]

        image_bytes = base64.b64decode(logo_base64)

        logo_key = f"logos/{project_id}/logo.png"

        s3.put_object(
            Bucket="cammi-devprod",
            Key=logo_key,
            Body=image_bytes,
            ContentType="image/png"
        )

        print(f"✅ Logo saved at s3://cammi-devprod/{logo_key}")
        return logo_key

    except Exception as e:
        print(f"❌ Failed to save logo: {e}")
        return None

def get_project_logo_from_s3(project_id: str):
    """
    Loads project logo from S3 and returns BytesIO.
    Returns None if logo does not exist.
    """
    try:
        logo_key = f"logos/{project_id}/logo.png"

        response = s3.get_object(
            Bucket="cammi-devprod",
            Key=logo_key
        )

        return BytesIO(response["Body"].read())

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print("ℹ️ No logo found for project")
            return None
        raise e


def add_logo_to_header_from_s3(document, project_id: str):
    """
    Adds project logo (if exists) to the TOP-RIGHT of the header.
    """
    try:
        logo_stream = get_project_logo_from_s3(project_id)
        if not logo_stream:
            return  # no logo → do nothing

        header = document.sections[0].header

        # Create a new paragraph ONLY for logo
        p = header.add_paragraph()
        p.alignment = 2  # RIGHT alignment

        run = p.add_run()
        run.add_picture(logo_stream, width=Inches(0.3))

    except Exception as e:
        print(f"⚠️ Logo skipped: {e}")



def lambda_handler(event, context):
    user_id = event.get("user_id", "")
    project_id = event.get("project_id", "")
    session_id = event.get("session_id", "")
    document_type = event.get("document_type", "")
    logo_base64 = event.get("logo_base64", "")

    common_metadata = {
    "user_id": user_id,
    "project_id": project_id,
    "document_type": document_type
    }


    template_bucket = 'cammi-devprod'
    object_key = f'flow/{user_id}/{document_type}/execution_plan.json'
    template_key = f'flow/{document_type}/marketing_document_template.json'
    output_bucket = 'cammi-devprod'

    timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    output_key = (
        f'project/{project_id}/{document_type}/'
        f'marketing_strategy_document/{document_type}_{timestamp}.docx'
    )
    knowledgebase_output = f'knowledgebase/{user_id}/{user_id}_{document_type}_{timestamp}.docx'

    # -------------------------------
    # 1️⃣ SAVE LOGO (ONLY IF SENT)
    # -------------------------------
    if logo_base64:
        save_project_logo_to_s3(project_id, logo_base64)

    # Load template JSON
    template = read_json_from_s3(template_bucket, template_key)

    # Load DOCX template from S3
    document = load_template_from_s3(
        'cammi-devprod',
        'templates/cammi_master_strategy_template.docx'
    )

    # -------------------------------
    # 2️⃣ HEADER (TEXT + LOGO)
    # -------------------------------
    header = document.sections[0].header

    # Clear ALL header paragraphs safely
    for p in header.paragraphs:
        p.clear()

    # 👉 Add logo first (top-right)
    add_logo_to_header_from_s3(document, project_id)

    # 👉 Add header text (left aligned)
    p_text = header.add_paragraph()
    p_text.alignment = 0  # LEFT

    doc_type_full = DOCUMENT_TYPE_NAMES.get(document_type.lower(), document_type)

    run_header = p_text.add_run(f"Cammi.ai | {doc_type_full} Document")
    run_header.font.size = Pt(10)
    run_header.font.bold = True
    run_header.font.color.rgb = RGBColor(50, 50, 50)

    # -------------------------------
    # 3️⃣ FOOTER (UNCHANGED)
    # -------------------------------
    footer = document.sections[0].footer
    footer_paragraph = footer.paragraphs[0]
    footer_paragraph.clear()

    run_page = footer_paragraph.add_run("Page ")
    fldChar1 = OxmlElement('w:fldChar')
    fldChar1.set(qn('w:fldCharType'), 'begin')
    run_page._r.append(fldChar1)

    instrText = OxmlElement('w:instrText')
    instrText.text = 'PAGE'
    run_page._r.append(instrText)

    fldChar2 = OxmlElement('w:fldChar')
    fldChar2.set(qn('w:fldCharType'), 'end')
    run_page._r.append(fldChar2)

    run_conf = footer_paragraph.add_run(
        " | From Clarification to Iteration: Move faster with CAMMI"
    )

    for r in [run_page, run_conf]:
        r.font.size = Pt(10)
        r.font.bold = True
        r.font.color.rgb = RGBColor(50, 50, 50)

    # -------------------------------
    # 4️⃣ RENDER CONTENT
    # -------------------------------
    for section in template:
        for subsection in section.get("sections", []):
            subheading = format_heading(subsection.get("subheading", ""))
            s3_path = subsection.get("s3_path", "")

            if not s3_path.startswith("s3://"):
                s3_path = (
                    f"s3://cammi-devprod/{project_id}/"
                    f"{document_type}/{s3_path}"
                )

            content_text = read_text_file_from_s3(s3_path)

            document.add_paragraph(subheading, style='Heading 1')
            add_formatted_paragraphs(
                document,
                content_text,
                template_h1=subheading
            )

    # -------------------------------
    # 5️⃣ SAVE DOC
    # -------------------------------
    buffer = BytesIO()
    document.save(buffer)
    buffer.seek(0)

    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=buffer.getvalue(),
        Metadata=common_metadata,
        ContentType=(
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
    )

    s3.put_object(
        Bucket=output_bucket,
        Key=knowledgebase_output,
        Body=buffer.getvalue(),
        Metadata=common_metadata,
        ContentType=(
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
        )
    )
    document_name = output_key.split('/')[-1]
    document_url = f"s3://{output_bucket}/{output_key}"

    save_document_history_to_dynamodb(
        user_id=user_id,
        project_id=project_id,
        document_type=document_type,
        document_name=document_name,
        document_url=document_url
    )

    update_status_to_false(
        bucket_name='cammi-devprod',
        object_key=object_key
    )

    return {
        'statusCode': 200,
        'session_id': session_id,
        'project_id': project_id,
        'message': f"DOCX document created at s3://{output_bucket}/{output_key}"
    }
