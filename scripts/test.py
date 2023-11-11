import logging
import boto3
from botocore.exceptions import ClientError
import os
import sys

def create_bucket(bucket_name: str) -> bool:
    """Create an S3 bucket
    :param bucket_name: Bucket to create
    :return: True if bucket was created, else False
    """
    s3_client = boto3.client('s3')
    try:
        response = s3_client.create_bucket(ACL='private', Bucket=bucket_name,CreateBucketConfiguration={'LocationConstraint': 'us-east-1'},)
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(file_name: str, bucket_name: str, object_name=None: str) -> bool:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True


if __name__ == "__main__":
    s3_bucket_name = sys.argv[1]
    s3_file_name = sys.argv[2]
    upload_file(file_name, bucket_name, object_name=None)