import logging
import boto3
from botocore.exceptions import ClientError
import os
import sys

def create_bucket(ACCESS_KEY: str, SECRET_KEY:str, SESSION_TOKEN: str, bucket_name: str) -> bool:
    """Create an S3 bucket
    :param bucket_name: Bucket to create
    :return: True if bucket was created, else False
    """
    s3_client = boto3.client('s3',  
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN)
    try:
        response = s3_client.create_bucket(ACL='private', Bucket=bucket_name)
        #,CreateBucketConfiguration={'LocationConstraint': 'us-east-1'},
    except ClientError as e:
        logging.error(e)
        return False
    return True


def upload_file(ACCESS_KEY: str, SECRET_KEY:str, SESSION_TOKEN: str, file_name: str, bucket_name: str) -> bool:
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket_name: Bucket to upload to
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',  
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN)
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def create_lambda_emr(ACCESS_KEY: str, SECRET_KEY:str, SESSION_TOKEN: str, postfix: str) -> bool:

    SOURCEARN = "arn:aws:s3:::landing-zone-" + postfix
    LANDINGZONE = "landing-zone-" + postfix

    s3_client = boto3.client('s3',  
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN)

    iam_client = boto3.client('iam',  
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN)

    lambda_client = boto3.client('lambda',  
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY,
    aws_session_token=SESSION_TOKEN, region_name='us-east-1')

    script_dir = os.path.dirname(__file__) 
    rel_path = "../code/transient_emr.zip"
    zip_file_path = os.path.join(script_dir, rel_path)

    with open(zip_file_path, 'rb') as f:
        zipped_code = f.read()
        
    role = iam_client.get_role(RoleName='LabRole')
    try:
        response = lambda_client.create_function(
            FunctionName = 'Create-transient-emr',
            Runtime = 'python3.8',
            Role = role['Role']['Arn'],
            Handler = 'transient_emr.lambda_handler',
            Code = dict(ZipFile = zipped_code),
            Description='Lambda function triggered by S3 file creation',
            Timeout = 300,
            Environment={
                'Variables': {
                    'postfix': postfix
            },
        },
        )
    except ClientError as e:
        logging.error(e)
        return False
    try:
        response2 = lambda_client.add_permission(
            Action='lambda:InvokeFunction',
            FunctionName='Create-transient-emr',
            Principal='s3.amazonaws.com',
            SourceArn=SOURCEARN,
            StatementId='s3',
        )


        s3_notification_config = {
            'LambdaFunctionConfigurations': [
                {
                'LambdaFunctionArn': response['FunctionArn'],
                'Events': ['s3:ObjectCreated:*'],
                }
            ]
        }
    
        s3_client.put_bucket_notification_configuration(
            Bucket = LANDINGZONE,
            NotificationConfiguration = s3_notification_config,
        )
        
    except ClientError as e:
        logging.error(e)
        return False
    return True 


if __name__ == "__main__":
    ACCESS_KEY = sys.argv[1]
    SECRET_KEY = sys.argv[2]
    SESSION_TOKEN = sys.argv[3]

    ## CHANGE THIS ###
    postfix = sys.argv[4]
    #s3_bucket_name = sys.argv[5]

    CODEBUCKET = "code-" + postfix
    LANDINGZONE = "landing-zone-" + postfix
    CLEANINGBUCKET = "cleaning-zone-" + postfix
    QUERYRESULTBUCKET = "query-result-" + postfix

    print("Creating Buckets ...")
    create_bucket(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, CODEBUCKET)
    create_bucket(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, LANDINGZONE)
    create_bucket(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, CLEANINGBUCKET)
    create_bucket(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, QUERYRESULTBUCKET)
    print("Finish creating")

    print("Uploading file ...")
    #upload_file(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, "../data_source/parquet/chess_game_MITCHELL.parquet", LANDINGZONE )
    upload_file(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, "../data_source/parquet/chess_game_REVEL.parquet", LANDINGZONE)
    upload_file(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, "../data_source/parquet/chess_game_SAHIT.parquet", LANDINGZONE)

    upload_file(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, "../code/transform.py", CODEBUCKET)
    print("Finish uploading")

    print("Creating Lambda")
    create_lambda_emr(ACCESS_KEY, SECRET_KEY, SESSION_TOKEN, postfix)
    print("Creating finish")
