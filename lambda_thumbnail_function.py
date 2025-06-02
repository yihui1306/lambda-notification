
import boto3
import os
import tempfile
import logging
from PIL import Image
import logging
s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('BUCKET_NAME', 'birds-detection-bucket')
THUMBNAIL_FOLDER = 'images/thumbnails/'
ORIGINAL_FOLDER = 'images/original/'
UPLOADS_FOLDER='images/uploads/'

def generate_thumbnail(image_path, size=(128, 128)):
    with Image.open(image_path) as img:
        img.thumbnail(size)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp:
            img.save(tmp.name, format='JPEG', quality=85)
            tmp.seek(0)
            return tmp.read()

def lambda_handler(event, context):
    for record in event['Records']:
        key = record['s3']['object']['key']
        if not key.startswith(UPLOADS_FOLDER):
            continue  

        original_key = key.replace(UPLOADS_FOLDER, ORIGINAL_FOLDER)
        copy_source = {
            'Bucket': BUCKET_NAME,
            'Key': key
        }
        s3.copy_object(
            Bucket=BUCKET_NAME,
            CopySource=copy_source,
            Key=original_key,
            ContentType='image/jpg' 
        )

        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        s3.download_file(BUCKET_NAME, key, tmp_file.name)

        thumbnail_bytes = generate_thumbnail(tmp_file.name)
        thumbnail_key = key.replace(UPLOADS_FOLDER, THUMBNAIL_FOLDER)
        s3.put_object(Bucket=BUCKET_NAME, Key=thumbnail_key, Body=thumbnail_bytes, ContentType='image/jpg')

        logging.info(f"Thumbnail created and uploaded to: {thumbnail_key}")
    return {
       'statusCode': 200,
       'body': 'UPLOADED SUCCESSFULLY !'
    }

