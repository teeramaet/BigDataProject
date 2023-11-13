import json
def kaggle_auth_setup(kaggle_auth, output_file):
  try:
    data = json.loads(kaggle_auth)
    with open(output_file, 'w') as file:
      json.dump(data, file, indent=4)
    print('setup successful')
  except:
    print("Setup Failed")

kaggle_auth_setup('{"username":"teeramaetbon","key":"0b3bad75225ff3d3ff22ce045a0d4918"}', '/root/.kaggle/kaggle.json')