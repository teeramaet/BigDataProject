import os
import boto3
def create_lambda_emr(ACCESS_KEY: str, SECRET_KEY:str, SESSION_TOKEN: str) -> bool:
  iam_client = boto3.client('iam')
  lambda_client = boto3.client('lambda')

  script_dir = os.path.dirname(__file__) #<-- absolute dir the script is in
  rel_path = "../code/transform.zip"
  zip_file_path = os.path.join(script_dir, rel_path)

  with open(zip_file_path, 'rb') as f:
    zipped_code = f.read()
    
  role = iam_client.get_role(RoleName='LabRole')
  response = lambda_client.create_function(
      FunctionName = 'Create-transient-emr',
      Runtime = 'python3.9',
      Role = role['Role']['Arn'],
      Handler = 'handler.lambda_handler',
      Code = dict(ZipFile = zipped_code),
      Timeout = 300, # Maximum allowable timeout
  )

