# BigDataProject

**Assuming you are using AWS Learner Lab and have the following roles: LabRole, EMR_DefaultRole, and EMR_EC2_DefaultRole, please follow these instructions:**
**Download the dataset from https://www.kaggle.com/datasets/arevel/chess-games.**
**Save the downloaded dataset to the "data_source/csv" directory. You can use a sample of the real dataset that we provided since GitHub does not allow the storage of large files, which is still suitable for testing purposes.**
**Rename the dataset to "chess_game_REVEL."**
**These steps will ensure that you have the dataset ready for your testing needs while adhering to GitHub's file size restrictions.**

## Set up your virtual environment name env

### For Window run

python -m venv env

### For MAC_OS/Linux

python3 -m venv env

## 1. Activate the python environment

### For Window

#### run this to activate

env\Scripts\activate

### MAC_OS/Linux

#### run this to activate

source env/bin/activate

#### run this to deactivate

deactivate

## 2. Install the package

pip install -r requirements.txt

## 3. Configure files to your setting

In code/transient_emr.py \
'Ec2KeyName': 'vockey',\
'Ec2SubnetId': 'subnet-03022b7fca1546530',\
LogUri="s3://aws-logs-580632447399-us-east-1/elasticmapreduce/",

Then, zip the transient_emr.py to transient_emr.zip

## 4. Run script to set up our the pipeline

cd scripts
python setup.py <YOUR ACCESS_KEY> <YOUR SESSION_KEY> <YOUR ACCESS_KEY> <A RANDOM NUMBER FOR AS A POSTFIX>

## 5. Set the destination bucket for athena query result

![Picture of setting athena](/images/querysetting.png "athena bucket setting")

## EXTRA if you want to explore the data by running

cd jupyter
jupyter notebook
