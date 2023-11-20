import os;
import boto3;

client = boto3.client('emr', region_name='us-east-1')

def lambda_handler(event, context):
    
    postfix = os.environ['postfix']
    CLEANINGZONE = "s3://cleaning-zone-" + postfix
    glue_client = boto3.client(
    'glue', 
    region_name = 'us-east-1')

    try:
        glue_client.create_crawler(
        Name = 'crawler name',
        Role = 'role to be used by glue to create the crawler',
        DatabaseName = 'database where the crawler should create the table',
        Targets = 
        {
            'S3Targets': 
            [
                {
                    'Path': CLEANINGZONE
                }
            ]
        }
        )

    except:
        glue_client.update_crawler(
        Name = 'crawler name',
        Role = 'role to be used by glue to create the crawler',
        DatabaseName = 'database where the crawler should create the table',
        Targets = 
        {
            'S3Targets': 
            [
                {
                    'Path': CLEANINGZONE
                }
            ]
        }
    )
        
    glue_client.start_crawler(
        Name = 'crawler name'
    )