import boto3
import os
from botocore.exceptions import ClientError
import logging


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


bucket_name = 'air-traffic-data'

raw_files_dir = os.path.join(os.path.dirname(__file__), '../raw_data')
file_names = os.listdir(raw_files_dir)
file_full_path = []
for file in file_names:
    file_full_path.append(os.path.join(raw_files_dir, file))

for file_path, file_name in zip(file_full_path, file_names):
    upload_file(file_path, bucket_name, file_name)
