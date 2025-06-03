# import boto3
# import os
# import json
#
# sns = boto3.client('sns')
# SNS_TOPIC_ARN = os.environ.get('arn:aws:sns:us-east-1:533267069889:new-bird-tag')
#
# def send_notifications(tags: dict, file_url: str):
#     """
#     Publish a tag-based notification to SNS when a file is uploaded and tagged.
#     """
#     try:
#         species = list(tags.keys())
#         species_str = ", ".join(species)
#
#         message = {
#             "message": f"A new file has been uploaded containing the following bird species: {species_str}.",
#             "tags": tags,
#             "file_url": file_url
#         }
#
#         response = sns.publish(
#             TopicArn=SNS_TOPIC_ARN,
#             Subject=f"New Bird Detection Updated in the System: {species_str}",
#             Message=json.dumps(message)
#         )
#
#         print(f"[SNS] Notification published. MessageId: {response.get('MessageId')}")
#     except Exception as e:
#         print(f"[SNS ERROR] Failed to publish notification: {e}")
