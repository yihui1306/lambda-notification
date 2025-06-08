import boto3
import json
import uuid
import os
import cgi
import tempfile
from urllib.parse import unquote_plus
import requests
from decimal import Decimal
import base64
from io import BytesIO
import traceback
import mimetypes

s3 = boto3.client('s3')
sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'birds-detection-bucket')
TABLE_NAME = os.environ.get('TABLE_NAME', 'birds-detection-data')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

table = dynamodb.Table(TABLE_NAME)


# Helper functions

def get_user_email_from_event(event):
    try:
        auth_header = event["headers"].get("Authorization") or event["headers"].get("authorization")
        if not auth_header:
            print("[WARNING] No Authorization header found")
            return "UnknownUser"

        token = auth_header.split(" ")[1]
        padded = token.split('.')[1] + '=='
        decoded = base64.b64decode(padded)
        payload = json.loads(decoded)
        email = payload.get("email", "UnknownUser")
        print(f"[DEBUG] Extracted email from token: {email}")
        return email
    except Exception as e:
        print(f"[WARNING] Failed to extract user email: {e}")
        return "UnknownUser"


def convert_decimals(obj):
    if isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    return obj


def parse_content(content):
    try:
        return json.loads(content)
    except:
        pass

    tags = []
    for line in content.strip().split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        if ':' in line:
            tag, count = line.split(':', 1)
            count = int(count.strip()) if count.strip().isdigit() else 1
        else:
            tag, count = line, 1

        tags.append({tag.strip(): count})

    return tags if tags else None


def sanitize_tags(tags):
    safe_tags = {}
    for k, v in tags.items():
        if isinstance(k, str) and isinstance(v, (int, float)) and not (
                v is None or isinstance(v, float) and (v != v)):  # NaN check
            safe_tags[k] = v
    return safe_tags


def detect_birds_tags(file_path, file_type, image_url=None):
    print(f"Triggered detect birds")
    audio_url = f"http://3.95.164.117:8000/analyze-audio"
    files = {}
    data = {}

    try:
        if file_type == "audio":
            with open(file_path, "rb") as f:
                files["file"] = (os.path.basename(file_path), f, "audio/wav")
                response = requests.post(audio_url, files=files, timeout=360)

        else:
            print(f"[ERROR] Unsupported file_type: {file_type}")
            return {}

        response.raise_for_status()
        result = response.json()

        print("[DEBUG detect_birds_tags] API response json:", result)
        print("[DEBUG detect_birds_tags] tags:", result.get("tags", {}))

        return result.get("tags", {})

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] API request failed: {e}")
        return {}


def lambda_handler(event, context):
    if 'Records' in event and 's3' in event['Records'][0]:
        return handle_trigger_s3(event)
    elif 'httpMethod' in event:
        resource = event.get('resource', '')
        path = event.get("path", "")
        http_method = event.get('httpMethod', '')
        if http_method == 'GET' and resource == '/api/status':
            return handle_api_status(event)
        elif (http_method == 'GET' or http_method == 'POST') and resource == '/api/search-tags':
            return handle_search_by_tags(event)
        elif http_method == 'POST' and resource == "/api/search-species":
            return handle_search_by_species(event)
        elif http_method == 'POST' and resource == '/api/get-original-from-thumbnail':
            return handle_get_original_from_thumbnail(event)
        elif http_method == 'POST' and resource == '/api/query-from-file':
            # return handle_query_from_tags_file(event)
            return handle_query_from_media(event, context)
        elif http_method == 'POST' and resource == '/api/delete-files':
            return handle_delete_files(event)
        elif http_method == 'POST' and resource == '/api/manual-tagging':
            return handle_manual_tagging(event)
        elif http_method == 'POST' and resource == '/api/uploads':
            return uploads_handler(event)
        else:
            return {
                "statusCode": 404,
                "headers": {
                    'Content-Type': 'application/json',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                },
                "body": json.dumps({"error": "Resource not found"})
            }
    else:
        return {
            "statusCode": 400,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Unknown event source"})
        }


# def get_user_id(event):
#     #get user email as user id
#     print(event["Records"])
#     try:
#         return event["Records"]["authorizer"]["claims"]["email"]
#     except KeyError:
#         return "UnknownUser"

def handle_trigger_s3(event):
    print(f"[DEBUG] S3 trigger event received: {json.dumps(event)}")
    for record in event['Records']:
        key = unquote_plus(record['s3']['object']['key'])
        print(f"[DEBUG] Processing S3 object key: {key}")
        file_type = ''
        if key.startswith('audio/') or key.endswith(('.wav', '.mp3')):
            file_type = 'audio'
        # new
        user_id = ''
        try:
            head = s3.head_object(Bucket=BUCKET_NAME, Key=key)
            print(f"[DEBUG] S3 object metadata: {head.get('Metadata', {})}")
            user_id = head.get("Metadata", {}).get("user_id", "UnknownUser") or head.get("Metadata", {}).get("user-id",
                                                                                                             "UnknownUser")
            print(f"[DEBUG] Extracted user_id: {user_id}")
        except Exception as e:
            print(f"[ERROR] Failed to get S3 metadata for key {key}: {e}")
            user_id = "UnknownUser"

        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        s3.download_file(BUCKET_NAME, key, tmp_file.name)

        tags = detect_birds_tags(tmp_file.name, file_type)
        print(f"[DEBUG] Raw API response tags: {tags}")
        print("[DEBUG handler] Tags returned from detection:", tags)

        # debug
        # Before passing to sanitize_tags, ensure tags is not None
        tags = tags or {"unknown_bird": 1}
        tags = sanitize_tags(tags)

        # handle notification logic
        try:
            response = table.scan()
            all_existing_tags = set()
            for item in response.get('Items', []):
                item_tags = item.get('tags', {})
                all_existing_tags.update(tag.lower() for tag in item_tags)

            new_species = [tag for tag in tags if tag.lower() not in all_existing_tags]

            if new_species:
                bird_species_str = ", ".join(new_species)
                message_email = f"The new bird species has been updated: {bird_species_str}."
                # message_text = message.key + message.value

                sns.publish(
                    TopicArn="arn:aws:sns:us-east-1:301627179176:birdtag-sns",
                    Subject=f"New Bird Species Alert: {bird_species_str}",
                    Message=message_email
                )
                print(f"[SNS] Notification published for new tags: {new_species}")
            else:
                print("[SNS] No new species detected — no notification sent.")
        except Exception as e:
            print(f"[ERROR] Failed to check or send SNS notification: {e}")

        s3_url = f"s3://{BUCKET_NAME}/{key}"
        thumbnail_url = None
        original_url = None

        if file_type == 'audio':
            original_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"
            item = {
                'id': key,
                'user_id': user_id,  # new
                'original_url': original_url,
                'type': file_type,
                'thumbnail_url': "NO_URL",
                'tags': tags or {"unknown_bird": 1}
            }
            table.put_item(Item=item)

    return {
        'statusCode': 200,
        "headers": {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        },
        'body': 'Metadata stored in DynamoDB.'
    }


def handle_search_by_tags(event):
    cors_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,POST,OPTIONS"
    }
    if event['httpMethod'] == 'OPTIONS':
        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": ""
        }
    if event['httpMethod'] == 'GET':
        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": "you reach the get endpoint"
        }
        # tags = {}
        # params = event.get('queryStringParameters', {})
        # if params:
        #     for key, value in params.items():
        #         if key.startswith('tag') and f'count{key[3:]}' in params:
        #             tag = value
        #             count = int(params.get(f'count{key[3:]}', 0))
        #             tags[tag] = count
    elif event['httpMethod'] == 'POST':
        try:
            tags = json.loads(event.get('body', '{}'))
        except:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": "Invalid JSON in POST body"})
            }
    else:
        return {
            "statusCode": 405,
            "headers": cors_headers,
            "body": json.dumps({"error": "Method not allowed"})
        }

    if not tags:
        return {
            "statusCode": 400,
            "headers": cors_headers,
            "body": json.dumps({"error": "No tags provided"})
        }

    try:
        response = table.scan()
        matches = []
        for item in response.get('Items', []):
            item_tags = item.get('tags', {})
            if all(item_tags.get(tag, 0) >= count for tag, count in tags.items()):
                if item['type'] == 'image':
                    matches.append({"original_url": item['original_url'], "thumbnail_url": item['thumbnail_url'],
                                    "type": item['type']})
                else:
                    matches.append({"original_url": item['original_url'], "thumbnail_url": item['thumbnail_url'],
                                    "type": item['type']})

        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": json.dumps({"data": matches})
        }

    except Exception as e:
        print(f"[ERROR] DynamoDB query failed: {e}")
        return {
            "statusCode": 500,
            "headers": cors_headers,
            "body": json.dumps({"error": "Internal server error"})
        }


def handle_api_status(event):
    return {
        "statusCode": 200,
        "headers": {
            'Content-Type': 'application/json',
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Allow-Methods": "OPTIONS,GET"
        },
        "body": json.dumps({"status": "API is running"})
    }


def handle_search_by_species(event):
    if event['httpMethod'] != 'POST':
        return {
            "statusCode": 405,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Method not allowed"})
        }

    try:
        species_tags = json.loads(event.get('body', '[]'))
        if not isinstance(species_tags, list) or not all(isinstance(tag, str) for tag in species_tags):
            return {
                "statusCode": 400,
                "headers": {
                    'Content-Type': 'application/json',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                },
                "body": json.dumps({"error": "Request body must be a list of tag strings"})
            }
    except:
        return {
            "statusCode": 400,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Invalid JSON in POST body"})
        }

    if not species_tags:
        return {
            "statusCode": 400,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "No tags provided"})
        }

    try:
        response = table.scan()
        matches = []

        for item in response.get('Items', []):
            item_tags = item.get('tags', {})
            if any(tag in item_tags and item_tags[tag] > 0 for tag in species_tags):
                matches.append({
                    "original_url": item.get('original_url'),
                    "thumbnail_url": item.get('thumbnail_url'),
                    "type": item.get('type')
                })

        return {
            "statusCode": 200,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"data": matches})
        }

    except Exception as e:
        print(f"[ERROR] DynamoDB query failed: {e}")
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Internal server error"})
        }


def handle_get_original_from_thumbnail(event):
    if event['httpMethod'] != 'POST':
        return {
            "statusCode": 405,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Method not allowed"})
        }
    try:
        body = json.loads(event.get('body', '{}'))
        thumbnail_url = body.get('thumbnail_url')

        if not thumbnail_url:
            return {
                "statusCode": 400,
                "headers": {
                    'Content-Type': 'application/json',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                },
                "body": json.dumps({"error": "Missing 'thumbnail_url' in request body"})
            }

        response = table.scan()
        for item in response.get('Items', []):
            if item.get('thumbnail_url') == thumbnail_url:
                return {
                    "statusCode": 200,
                    "headers": {
                        'Content-Type': 'application/json',
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Headers": "Content-Type, Authorization",
                        "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                    },
                    "body": json.dumps({
                        "original_url": item.get("original_url"),
                        "thumbnail_url": thumbnail_url,
                        "type": item.get("type")
                    })
                }

        return {
            "statusCode": 404,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Thumbnail not found"})
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch original from thumbnail: {e}")
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Internal server error"})
        }


##########################################

def handle_query_from_tags_file(event):
    if event['httpMethod'] != 'POST':
        return {"statusCode": 405,
                "body": json.dumps({"error": "Method not allowed"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
                }

    try:
        body = event.get('body', '')

        if event.get('isBase64Encoded'):
            body = base64.b64decode(body).decode('utf-8')

        if 'form-data' in body:
            lines = body.split('\n')
            content = []
            found_content = False

            for line in lines:
                if not line.strip() and not found_content:
                    found_content = True
                    continue
                if found_content and not line.startswith('--'):
                    content.append(line.strip())
                elif found_content and line.startswith('--'):
                    break

            body = '\n'.join(content).strip()

        tag_list = parse_content(body)
        if not tag_list:
            return {"statusCode": 400,
                    "body": json.dumps({"error": "No valid tags found"}),
                    "headers": {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                        'Access-Control-Allow-Methods': 'POST,OPTIONS'
                    }
                    }

        response = table.scan()
        matches = []
        seen = set()

        for tag_entry in tag_list:
            for tag, count in tag_entry.items():
                for item in response.get('Items', []):
                    item_tags = item.get('tags', {})
                    if item_tags.get(tag, 0) >= count:
                        url = item.get('original_url')
                        if url not in seen:
                            matches.append(convert_decimals({
                                "original_url": url,
                                "thumbnail_url": item.get('thumbnail_url'),
                                "type": item.get('type'),
                                "tags": item_tags
                            }))
                            seen.add(url)

        return {
            "statusCode": 200,
            "body": json.dumps({"data": matches}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500,
                "body": json.dumps({"error": "Server error"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
                }


# wen try
def handle_query_from_media(event, _ctx=None):
    cors_headers = {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "POST,OPTIONS"
    }
    # CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers}

    if event["httpMethod"] != "POST":
        return {"statusCode": 405, "headers": cors_headers,
                "body": json.dumps({"error": "Method not allowed"})}

    try:
        # ── 1. Decode & parse multipart ───────────────────────────────
        raw_body = base64.b64decode(event["body"]) if event.get("isBase64Encoded") else event["body"].encode()
        env = {  # fake WSGI env for cgi.FieldStorage
            "REQUEST_METHOD": "POST",
            "CONTENT_TYPE": event["headers"]["content-type"],
            "CONTENT_LENGTH": str(len(raw_body)),
        }
        form = cgi.FieldStorage(fp=BytesIO(raw_body), environ=env, keep_blank_values=True)

        # ── DEBUG START ────────────────────────────────────────────
        print("content-type header:", env["CONTENT_TYPE"])
        print("form.list ->", form.list)  # None means “no parts”
        if form.list:
            for i, part in enumerate(form.list):
                print(f"Part {i}:")
                print("  name     :", repr(part.name))
                print("  filename :", repr(part.filename))
                print("  type     :", repr(part.type))
                print("  headers  :", dict(part.headers))
                print("  value len:", len(part.value))
        # ── DEBUG END ──────────────────────────────────────────────

        file_item = form["file"]  # ← name attribute in the form
        file_bytes = file_item.file.read()

        # ── 2. Write to /tmp so TF/Torch can open it ──────────────────
        tmp_name = f"/tmp/{uuid.uuid4().hex}"
        with open(tmp_name, "wb") as f:
            f.write(file_bytes)

        # ── 3. Infer file_type & run detector ─────────────────────────
        ext = os.path.splitext(file_item.filename)[1].lower()
        mime = mimetypes.guess_type(file_item.filename)[0] or ""
        file_type = ("image" if mime.startswith("image") else
                     "video" if mime.startswith("video") else
                     "audio" if mime.startswith("audio") else "unknown")

        tags = detect_birds_tags(tmp_name, file_type)  # ← returns {tag: count}
        os.remove(tmp_name)  # clean up tmp

        # ── 4. Query DynamoDB for matches ────────────────────────────
        response = table.scan()
        matches = []
        for item in response.get("Items", []):
            item_tags = item.get("tags", {})
            if all(item_tags.get(t, 0) >= c for t, c in tags.items()):
                matches.append(convert_decimals({
                    "original_url": item["original_url"],
                    "thumbnail_url": item["thumbnail_url"],
                    "type": item["type"]
                }))

        return {"statusCode": 200, "headers": cors_headers,
                "body": json.dumps({"data": matches})}

    except Exception as e:
        print("[ERROR]", e)
        return {"statusCode": 500, "headers": cors_headers,
                "body": json.dumps({"error": "Internal server error"})}


##########################################


def handle_delete_files(event):
    try:
        body = json.loads(event.get("body", "{}"))
        urls = body.get("urls", [])

        # new
        user_id = get_user_email_from_event(event)

        if not urls or not isinstance(urls, list):
            return {
                "statusCode": 400,
                "headers": {
                    'Content-Type': 'application/json',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                },
                "body": json.dumps({"error": "Missing or invalid 'urls' list in request"})
            }

        deleted_items = []

        for url in urls:

            if f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/" in url:
                key = url.split(f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/")[-1]
            elif url.startswith("s3://"):
                key = url.split(f"s3://{BUCKET_NAME}/")[-1]
            else:
                continue

            try:
                s3.delete_object(Bucket=BUCKET_NAME, Key=key)
            except Exception as s3_err:
                print(f"[ERROR] Failed to delete {key} from S3: {s3_err}")
                continue

            original_key = key
            if key.startswith("images/original/"):
                thumb_key = key.replace("images/original/", "images/thumbnails/")
                try:
                    s3.delete_object(Bucket=BUCKET_NAME, Key=thumb_key)
                except Exception as e:
                    print(f"[WARNING] Thumbnail not found or error deleting: {thumb_key}")

            try:
                table.delete_item(Key={"id": original_key, "user_id": user_id})
            except Exception as db_err:
                print(f"[ERROR] Failed to delete {key} from DynamoDB: {db_err}")
                continue

            deleted_items.append(key)

        return {
            "statusCode": 200,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({
                "message": "Files deleted successfully",
                "deleted": deleted_items
            })
        }

    except Exception as e:
        print(f"[ERROR] {e}")
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Internal server error"})
        }


def handle_manual_tagging(event):
    try:
        body = json.loads(event.get('body', '{}'))
        urls = body.get('url')
        operation = body.get('operation')
        tag_list = body.get('tags')

        if not isinstance(urls, list) or not isinstance(tag_list, list):
            return {
                "statusCode": 400,
                "headers": {
                    'Content-Type': 'application/json',
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type, Authorization",
                    "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
                },
                "body": json.dumps({"error": "Invalid format for 'url' or 'tags'. Must be lists."})
            }

        tags = {}
        for tag_entry in tag_list:
            if ',' in tag_entry:
                tag, count = tag_entry.split(',', 1)
                try:
                    tags[tag.strip()] = int(count.strip())
                except ValueError:
                    continue

                    # new add user id
        user_id = get_user_email_from_event(event)

        for url in urls:
            key = url.split(f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/")[-1]
            response = table.get_item(Key={'id': key, "user_id": user_id})  # new
            item = response.get('Item')

            if not item:
                continue

            current_tags = item.get('tags', {})

            if operation == 1:
                for tag, count in tags.items():
                    current_tags[tag] = current_tags.get(tag, 0) + count
            elif operation == 0:
                for tag in tags:
                    if tag in current_tags:
                        del current_tags[tag]

            item['tags'] = current_tags
            table.put_item(Item=item)

        return {
            "statusCode": 200,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"message": "Tags updated successfully"})
        }

    except Exception as e:
        print(f"[ERROR] {e}")
        return {
            "statusCode": 500,
            "headers": {
                'Content-Type': 'application/json',
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type, Authorization",
                "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
            },
            "body": json.dumps({"error": "Internal server error"})
        }


def uploads_handler(event):
    try:
        # CORS headers
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Content-Type': 'application/json'
        }

        # Handle CORS preflight
        if event.get('httpMethod') == 'OPTIONS':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': ''
            }

        if event.get('httpMethod') != 'POST':
            return {
                'statusCode': 405,
                'headers': headers,
                'body': json.dumps({'error': 'Method not allowed'})
            }

        # Parse request body
        try:
            body = json.loads(event.get('body', '{}'))
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error': 'Invalid JSON body'})
            }

        file_name = body.get('fileName')
        file_type = body.get('fileType')

        if not file_name or not file_type:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error': 'fileName and fileType are required'})
            }

        # Generate unique key for the file in images/uploads/ folder
        # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        timestamp = "05062025"
        # unique_id = str(uuid.uuid4())[:8]
        unique_id = "001"
        # Clean filename to avoid issues
        clean_filename = file_name.replace(' ', '_').replace('(', '').replace(')', '')
        # key = f"images/uploads/{timestamp}_{unique_id}_{clean_filename}"

        # for deteted upload file
        if file_type.startswith('image'):
            key = f"images/uploads/{timestamp}_{unique_id}_{clean_filename}"
        elif file_type.startswith('video'):
            key = f"videos/{timestamp}_{unique_id}_{clean_filename}"
        else:
            return {
                'statusCode': 400,
                'headers': headers,
                'body': json.dumps({'error': 'Unsupported fileType'})
            }

        # new Get user email before using it
        userEmail = get_user_email_from_event(event)
        print(f"[INFO] Presigned URL created by: {userEmail}")
        print(f"[INFO] User email extracted: {userEmail}")

        # Generate presigned URL for PUT operation
        presigned_url = s3.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': BUCKET_NAME,
                'Key': key,
                'ContentType': file_type,
                'Metadata': {
                    'user_id': userEmail
                }
            },
            ExpiresIn=300
        )

        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'presignedUrl': presigned_url,
                'key': key,
                'user_id': userEmail,  # new
                'expiresIn': 300
            })
        }

    except Exception as e:
        print(f"Error generating presigned URL: {str(e)}")

        return {
            'statusCode': 500,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST, OPTIONS',
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'error': 'Failed to generate upload URL',
                'message': str(e)
            })
        }