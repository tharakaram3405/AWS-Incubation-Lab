from datetime import datetime
import requests, json, os, boto3
from requests.auth import HTTPBasicAuth

#importing environmental variables from Configuration
# Airflow credentials
username    = os.environ['username']
password    = os.environ['password']

ec2_DNS     = os.environ['ec2_DNS']       # EC2 host name
dag_id      = os.environ['dag_id']        # Dag ID
config_data = os.environ['config_data']   # Configuration details


def lambda_handler(event, context):
    print(event)
    path      = event['Records'][0]['s3']
    Bucket    = path['bucket']['name']
    key       = path['object']['key']
    filename  = key.split("/")[-1].split(".")[0]

    headers = {'Content-Type': 'application/json'}
    url     = 'http://' + ec2_DNS + ':8080/api/v1/dags/' + dag_id + '/dagRuns'
    auth    = HTTPBasicAuth(username, password)

    modified_config_data = json.loads(config_data)
    modified_config_data.update({'key' : key})
    modified_config_data.update({'Bucket' : Bucket})
    modified_config_data.update({'filename' : filename})

    print(modified_config_data)

    body = json.dumps({
        'conf' : modified_config_data,
        "dag_run_id": "lambda_run_"+ datetime.utcnow().isoformat()
    })
    try:
        print("Miracle No.1 : Upload event invoked Lambda function.")
        resp = requests.post(url, data = body, \
        headers = headers, auth = auth, verify = False)
        print(resp.text)
        print("Miracle No.2 : Lambda function triggered Airflow Dag.")
        return resp.status_code
        
    except Exception as e:
        return "Connection Error" + str(e)