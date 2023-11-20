import os;
import boto3;

glue_client = boto3.client('glue', region_name = 'us-east-1')

def lambda_handler(event, context):
    
    postfix = os.environ['postfix']
    role = os.environ['role']
    CLEANINGZONE = "s3://cleaning-zone-" + postfix
    DATABASENAME = "chessdb-" + postfix

    try:
        response = glue_client.create_crawler(
        Name = 'chess-clean-crawler',
        Role = role,
        DatabaseName = DATABASENAME,
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
        response2 = glue_client.update_crawler(
        Name = 'chess-clean-crawler',
        Role = role,
        DatabaseName = DATABASENAME,
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
        
    response3 = glue_client.start_crawler(
        Name = 'chess-clean-crawler'
    )