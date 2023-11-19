import os;
import boto3;

client = boto3.client('emr', region_name='us-east-1')

def lambda_handler(event, context):
    
    # location of code to run
    postfix = os.environ['postfix']
    backend_code = "s3://code-" + postfix + "/transform.py"
    # define the job details, location of script to run etc
    spark_submit = [
    '/usr/bin/spark-submit',
    '--master', 'yarn',
    '--deploy-mode', 'cluster',
    backend_code,
    postfix,
    ]
    print("Spark Submit : ",spark_submit)
    # define the cluster
    cluster_id = client.run_job_flow(
    Name = "transient_demo_testing",
    Instances = {
    'InstanceGroups': [
    {
    'Name': "Master",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'MASTER',
    'InstanceType': 'm4.large',
    'InstanceCount': 1,
    },
    {
    'Name': "Slave",
    'Market': 'ON_DEMAND',
    'InstanceRole': 'CORE',
    'InstanceType': 'm4.large',
    'InstanceCount': 2,
    }
    ],
    'Ec2KeyName': 'vockey',
    'KeepJobFlowAliveWhenNoSteps': False,
    'TerminationProtected': False,
    'Ec2SubnetId': 'subnet-03022b7fca1546530',
    },
    LogUri="s3://aws-logs-580632447399-us-east-1/elasticmapreduce/",
    ReleaseLabel= 'emr-5.33.1',
    Steps=[{"Name": "testJob",
    'ActionOnFailure': 'CONTINUE',
    'HadoopJarStep': {
    'Jar': 's3n://elasticmapreduce/libs/script-runner/script-runner.jar',
    # passed to the backend code
    'Args': spark_submit
    }
    }],
    BootstrapActions=[], 
    
    VisibleToAllUsers=True,
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    Applications = [ {'Name': 'Spark'},{'Name':'Hive'}])