# BigDataProject
# ASSUME that you use learner lab and you have these role LabRole, EMR_DefaultRole, EMR_EC2_DefaultRole
# The file in data_source/csv is a sample of the real big file. This is because github is not allow to store large fiile and this is good for test purpose

# So, please download the data set from https://www.kaggle.com/datasets/arevel/chess-games and put it in data_source/csv and name chess_game_REVEL

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
In code/transient_emr.py
    'Ec2KeyName': 'vockey',
    'Ec2SubnetId': 'subnet-03022b7fca1546530',
    LogUri="s3://aws-logs-580632447399-us-east-1/elasticmapreduce/",

Then, zip the transient_emr.py to transient_emr.zip


## 4. Run script to set up our the pipeline
cd scripts
python setup.py <YOUR ACCESS_KEY> <YOUR SESSION_KEY> <YOUR ACCESS_KEY> <A RANDOM NUMBER FOR AS A POSTFIX>

## EXTRA if you want to explore the data by running
cd code
jupyter notebook


