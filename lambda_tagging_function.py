import base64
import email
from io import BytesIO

import boto3
import json
import os
import tempfile
from urllib.parse import unquote_plus
import requests
from decimal import Decimal
import sns

s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'birds-detection-bucket')
TABLE_NAME = os.environ.get('TABLE_NAME', 'birds-detection-data')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

table = dynamodb.Table(TABLE_NAME)


# Helper functions

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
    url = f"http://54.146.219.94:8000/predict/{file_type}"
    files = {}
    data = {}

    try:
        if file_type == "image":
            if file_path:
                with open(file_path, "rb") as f:
                    files["image_file"] = (os.path.basename(file_path), f)
                    response = requests.post(url, files=files, data=data, timeout=60)
            elif image_url:
                data["image_url"] = image_url
                response = requests.post(url, data=data, timeout=60)
            else:
                print("[ERROR] Provide either a file path or image URL.")
                return {}

        elif file_type == "video":
            with open(file_path, "rb") as f:
                files["video_file"] = (os.path.basename(file_path), f)
                response = requests.post(url, files=files, timeout=300)

        else:
            print(f"[ERROR] Unsupported file_type: {file_type}")
            return {}

        response.raise_for_status()
        result = response.json()
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
            return handle_query_from_tags_file(event)
        elif http_method == 'POST' and resource == '/api/delete-files':
            return handle_delete_files(event)
        elif http_method == 'POST' and resource == '/api/manual-tagging':
            return handle_manual_tagging(event)
        elif http_method == 'POST' and resource == '/api/uploads':
            return handle_uploads(event)
        else:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "Resource not found"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }

            }
    else:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Unknown event source"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }


def handle_trigger_s3(event):
    for record in event['Records']:
        key = unquote_plus(record['s3']['object']['key'])
        file_type = 'image' if key.startswith('images/original/') else 'video'

        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        s3.download_file(BUCKET_NAME, key, tmp_file.name)

        tags = detect_birds_tags(tmp_file.name, file_type)
        print(f"[DEBUG] Raw API response tags: {tags}")
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
                message_email = {
                    "message": f"The new bird species has been updated: {bird_species_str}."
                }

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
        if file_type == 'image':
            original_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"
            thumbnail_key = key.replace('images/original/', 'images/thumbnails/')
            thumbnail_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{thumbnail_key}"
            item = {
                'id': key,
                'user_id': 'User999',
                'original_url': original_url,
                'type': file_type,
                'thumbnail_url': thumbnail_url,
                'tags': tags or {"unknown_bird": 1}
            }
            table.put_item(Item=item)

        else:
            original_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"
            item = {
                'id': key,
                'user_id': 'User999',
                'original_url': original_url,
                'type': file_type,
                'thumbnail_url': "NO_URL",
                'tags': tags or {"unknown_bird": 1}
            }
            table.put_item(Item=item)

    return {
        'statusCode': 200,
        'body': 'Metadata stored in DynamoDB.',
        "headers": {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type,Authorization',
            'Access-Control-Allow-Methods': 'POST,OPTIONS'
        }
    }



def handle_search_by_tags(event):
    if event['httpMethod'] == 'GET':
        tags = {}
        params = event.get('queryStringParameters', {})
        if params:
            for key, value in params.items():
                if key.startswith('tag') and f'count{key[3:]}' in params:
                    tag = value
                    count = int(params.get(f'count{key[3:]}', 0))
                    tags[tag] = count
    elif event['httpMethod'] == 'POST':
        try:
            tags = json.loads(event.get('body', '{}'))
        except:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Invalid JSON in POST body"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
                }
            }
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method not allowed"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    if not tags:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No tags provided"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
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
            "body": json.dumps({"data": matches}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"[ERROR] DynamoDB query failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }


def handle_api_status(event):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json"
        },
        "body": json.dumps({"status": "API is running"}),

    }


def handle_search_by_species(event):
    if event['httpMethod'] != 'POST':
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method not allowed"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    try:
        species_tags = json.loads(event.get('body', '[]'))
        if not isinstance(species_tags, list) or not all(isinstance(tag, str) for tag in species_tags):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Request body must be a list of tag strings"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
            }
    except:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Invalid JSON in POST body"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    if not species_tags:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No tags provided"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
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
            "body": json.dumps({"data": matches}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"[ERROR] DynamoDB query failed: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }


def handle_get_original_from_thumbnail(event):
    if event['httpMethod'] != 'POST':
        return {
            "statusCode": 405,
            "body": json.dumps({"error": "Method not allowed"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }
    try:
        body = json.loads(event.get('body', '{}'))
        thumbnail_url = body.get('thumbnail_url')

        if not thumbnail_url:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'thumbnail_url' in request body"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
            }

        response = table.scan()
        for item in response.get('Items', []):
            if item.get('thumbnail_url') == thumbnail_url:
                return {
                    "statusCode": 200,
                    "body": json.dumps({
                        "original_url": item.get("original_url"),
                        "type": item.get("type")
                    }),
                    "headers": {
                        'Content-Type': 'application/json',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                        'Access-Control-Allow-Methods': 'POST,OPTIONS'
                    }
                }

        return {
            "statusCode": 404,
            "body": json.dumps({"error": "Thumbnail not found"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"[ERROR] Failed to fetch original from thumbnail: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }


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


def handle_delete_files(event):
    try:
        body = json.loads(event.get("body", "{}"))
        urls = body.get("urls", [])

        if not urls or not isinstance(urls, list):
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing or invalid 'urls' list in request"}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
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
                table.delete_item(Key={"id": original_key, "user_id": "User999"})
            except Exception as db_err:
                print(f"[ERROR] Failed to delete {key} from DynamoDB: {db_err}")
                continue

            deleted_items.append(key)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Files deleted successfully",
                "deleted": deleted_items
            }),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"[ERROR] {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
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
                "body": json.dumps({"error": "Invalid format for 'url' or 'tags'. Must be lists."}),
                "headers": {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Access-Control-Allow-Methods': 'POST,OPTIONS'
                }
            }

        tags = {}
        for tag_entry in tag_list:
            if ',' in tag_entry:
                tag, count = tag_entry.split(',', 1)
                try:
                    tags[tag.strip()] = int(count.strip())
                except ValueError:
                    continue

        for url in urls:
            key = url.split(f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/")[-1]
            response = table.get_item(Key={'id': key, "user_id": "User999"})
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
            "body": json.dumps({"message": "Tags updated successfully"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

    except Exception as e:
        print(f"[ERROR] {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal server error"}),
            "headers": {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                'Access-Control-Allow-Methods': 'POST,OPTIONS'
            }
        }

def handle_uploads(event):
    if event['httpMethod'] == 'OPTIONS':
        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": ""
        }

    try:
        # Decode multipart/form-data
        content_type = event['headers'].get('Content-Type') or event['headers'].get('content-type')
        body = base64.b64decode(event['body']) if event.get('isBase64Encoded', False) else event['body']
        msg = email.message_from_bytes(body if isinstance(body, bytes) else body.encode(), policy=email.policy.default)

        file_part = next((p for p in msg.walk() if "filename=" in (p.get("Content-Disposition") or "")), None)
        if not file_part:
            raise ValueError("No file found in upload")

        file_data = file_part.get_payload(decode=True)
        file_name = file_part.get_filename()
        mime_type = file_part.get_content_type()
        file_type = 'image' if mime_type.startswith("image/") else 'video'

        # upload to S3
        key = f"{'images' if file_type == 'image' else 'videos'}/original/{file_name}"
        s3.upload_fileobj(BytesIO(file_data), BUCKET_NAME, key)

        # tag detect
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        tmp_file.write(file_data)
        tmp_file.flush()
        tags = sanitize_tags(detect_birds_tags(tmp_file.name, file_type))

        # Store metadata in DynamoDB
        original_url = f"https://{BUCKET_NAME}.s3.{REGION}.amazonaws.com/{key}"
        thumbnail_url = original_url.replace("/original/", "/thumbnails/") if file_type == 'image' else "NO_URL"

        table.put_item(Item={
            'id': key,
            'user_id': 'User999',
            'original_url': original_url,
            'type': file_type,
            'thumbnail_url': thumbnail_url,
            'tags': tags or {"unknown_bird": 1}
        })

        return {
            "statusCode": 200,
            "headers": cors_headers(),
            "body": json.dumps({"message": "Upload success", "file_url": original_url})
        }

    except Exception as e:
        print(f"[ERROR] Upload handler failed: {e}")
        return {
            "statusCode": 500,
            "headers": cors_headers(),
            "body": json.dumps({"error": "Upload failed"})
        }

def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
        "Access-Control-Allow-Methods": "OPTIONS,POST"
    }