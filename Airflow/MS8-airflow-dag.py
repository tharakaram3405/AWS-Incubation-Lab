# Import standard packages
import requests, logging, json
import boto3, time, io, re, decimal
import pandas as pd
import pyarrow as py
from datetime import datetime, timedelta, date
from botocore.exceptions import ClientError, ConnectionError

# Importing modules from Airflow package
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

global emr, s3, s3_resource, ec2, region
s3 = boto3.client("s3")
s3_resource = boto3.resource("s3")
region = 'ap-southeast-1'
emr = boto3.client("emr", region_name = region)
ec2 = boto3.client("ec2", region_name = region)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True
}

# Python functions
def get_Config(**kwargs):
    """
    This python function fetches the configuration
    return : Configuration in JSON format
    """
    s3 = boto3.resource('s3')
    dag_run = kwargs.get('dag_run')
    config_path = dag_run.conf["app_config_file_path"]
    bucketname=config_path.split("/")[2]
    filename=("/").join(config_path.split("/")[3:])
    content_object = s3.Object(bucketname, filename)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    return json_content
	
def copy_Data(**kwargs):
    """
    This Function copies the dataset from landing-zone s3 bucket to raw-zone bucket
    """
    source_bucket = kwargs['dag_run'].conf['Bucket']
    key=kwargs['dag_run'].conf['key']
    copy_source = {'Bucket': source_bucket, 'Key': key}
    bucket = s3_resource.Bucket("tharak-raw-zone")
    bucket.copy(copy_source, key)

def pre_Validation(**kwargs):
    """
    This function validates the following checks with the data in raw-zone:
    1. Data Availability check
    2. Data Count check
    3. Data Type check
    """
    s3_obj = s3_resource
    ti = kwargs['ti']
    path = kwargs['dag_run'].conf['key']
    bucket = "tharak-raw-zone"
    my_bucket = s3_obj.Bucket(bucket)
    
    for obj in my_bucket.objects.filter(Prefix='/'.join(path.split('/')[:-1])+"/"):
        if obj.key.strip().split("/")[-1]=="":
            continue
        buffer = io.BytesIO()
        object = s3_obj.Object(bucket,obj.key)
        object.download_fileobj(buffer)
        df = pd.read_parquet(buffer, engine = 'pyarrow')
    
        if df.count()[0]==0:
            raise Exception("Data Validation Exception : No Data found in the dataset")

        print("Data available in the file")
        pre_validation_data = dict()

        #Adding count of df into the data dictoniary
        pre_validation_data['counts'] = str(int(pre_validation_data.get('counts','0'))+df.count()[0])
    # ti.xcom_push(key = "pre_validation_data", value = pre_validation_data)
    return pre_validation_data

def get_security_group(group_name):
    """
    Function fetches the ID for the given security group name.
    return : Security Group ID
    """
    res = ec2.describe_security_groups(GroupNames = [group_name])
    return res['SecurityGroups'][0]['GroupId']

def create_Emr_Cluster(**kwargs):
    """
    A Python function which creates an EMR cluster.
    """
    try:
        emr_master_security_group_id = get_security_group("default")
        emr_slave_security_group_id = get_security_group("default")

        cluster_resp = emr.run_job_flow(
            Name = "Tharak-EMR-Cluster",
            LogUri = "s3://tharak-landing-zone/logs",
            ReleaseLabel = "emr-6.2.0",
            Applications=[
                    { 'Name': 'hadoop' },
                    { 'Name': 'spark' },
                    { 'Name': 'livy' },
                    { 'Name': 'hive' }
                ],                         
            Instances={
                'InstanceGroups': [
                    {
                        'Name': "Master",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': "Slave",
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    }
                ],
                'Ec2KeyName': 'tharak-key-pair',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
                'EmrManagedMasterSecurityGroup': emr_master_security_group_id,
                'EmrManagedSlaveSecurityGroup': emr_slave_security_group_id,
            },
            BootstrapActions =[
            {
                "Name": "custom_action",
                "ScriptBootstrapAction": {
                "Path": "s3://tharak-landing-zone/dependencyfiles/dependencies.sh"
                }
            }
        ],
            VisibleToAllUsers = True,
            JobFlowRole = 'EMR_EC2_DefaultRole',
            EbsRootVolumeSize = 32,
            ServiceRole = 'EMR_DefaultRole',
        )
        # Returns the cluster-ID
        cluster_id = cluster_resp['JobFlowId']
        print("Created cluster %s.", cluster_id)
    except ClientError:
        logging.exception("Couldn't create cluster.")
        raise
    else:
        return cluster_id

def cluster_Ready_Acknowledge(**kwargs):
    """
    Function waits for the acknowledgement of creation of transient cluster 
    """
    cluster_id = kwargs['ti'].xcom_pull(task_ids = "EMR_cluster_creation")
    emr.get_waiter('cluster_running').wait(ClusterId = cluster_id)

def get_ClusterDNS(**kwargs):
    """
    Function fetch the Master DNS name of the transient cluster created.
    returns : Master-Node DNS
    """
    try:
        cluster_id = kwargs['ti'].xcom_pull(task_ids = "EMR_cluster_creation")
        resp = emr.describe_cluster(ClusterId = cluster_id)
        master_dns = resp['Cluster']['MasterPublicDnsName']
        print("MasterDns obtained from response", master_dns)
    except ClientError:
        logging.exception("No MasterDns Name")

    else:
        return master_dns

def submit_livy(**kwargs):
    """
    This function submits the spark-job through Livy protocol (REST API).
    return  : API-response headers.
    """
    master_DNS = kwargs['ti'].xcom_pull(task_ids='get_Cluster_DNS')
    spark_file = kwargs['dag_run'].conf["spark_job_path"]
    datasets = kwargs["dag_run"].conf["filename"]
    host = "http://" + master_DNS + ":8998/batches"
    data = { "file" : spark_file, "className" : "com.example.SparkApp", 'args' : [datasets]}
    headers = { 'Content-Type' : 'application/json' }
    try:
        response = requests.post(host , data = json.dumps(data), headers = headers )
    except (ConnectionError, ValueError):
        return submit_livy()
    else:
        response_headers = dict(response.headers)
        print(response_headers)
        return response_headers

def livy_spark_job_status(**kwargs):
    """
    This function awaits for the status of submitted spark job through livy.
    return  : Spark Job completion status
    """
    master_DNS = kwargs['ti'].xcom_pull(task_ids='get_Cluster_DNS')
    response_headers = kwargs['ti'].xcom_pull(task_ids='submit_spark_through_livy')
    livy_status = ''
    host = 'http://' + master_DNS + ':8998'
    session_url = host + response_headers['Location']

    # Poll the status of the submitted  code
    while livy_status != 'success':
        livy_url = host + response_headers['Location']
        livy_response = requests.get(livy_url, headers={'Content-Type': 'application/json'}, verify = False, stream = True, timeout = 30)
        livy_status = livy_response.json()['state']
        
        if livy_status=='dead':
            raise Exception("Livy Session Dead!!!")            
        print('Statement status: ' + livy_status)

        #logging the logs
        lines = requests.get(session_url + '/log', headers={'Content-Type': 'application/json'}).json()['log']
        for line in lines:
            print(line)

        if 'progress' in livy_response.json():
            print('Progress: ' + str(livy_response.json()['progress']))
        time.sleep(5)
        
    final_livy_status = livy_response.json()['state']
    print(final_livy_status)
    return final_livy_status

def post_Validation(**kwargs):
    """
    This function validates the following checks with the data in raw-zone:
    1. Data Availability check
    2. Data Count check
    3. Data Type check
    """
    ti = kwargs['ti']       
    config_json         = ti.xcom_pull(task_ids='read_configurations')          # Pulling values from read_configurations function
    pre_validation_data = ti.xcom_pull(task_ids='pre_validation')               # Pulling pre-validation count from pre_validation function
    file_name           = kwargs['dag_run'].conf['filename']                    # getting filename details from dag_run conf
    transformed_data    = 'transformed-{dataset}'.format(dataset = file_name)    
    staging_zone_data_count = 0
    staging_path = config_json[transformed_data]['destination']['data-location'].replace(":","").split("/")
    
    s3_res = boto3.resource(staging_path[0])                            # s3 resource
    staging_bucket = s3_res.Bucket(staging_path[2])                     # s3 staging bucket name
    prefix = str(staging_path[3]) + '/' + str(staging_path[4])
    
    for obj in staging_bucket.objects.filter(Prefix=prefix):
        if (obj.key.endswith('parquet')):
            staging_df = pd.read_parquet('s3://' + obj.bucket_name + '/' + obj.key, engine='auto')           
            staging_zone_data_count += staging_df.count()[0] 
  
    if staging_df.empty:                                                # data availability check  
        raise Exception('No Data found in the dataset, Data Availability Exception!')

    print("Data Availability checked and confirmed !!!")
    
    pre_count = int(pre_validation_data['counts'])
    if pre_count == staging_zone_data_count:                            # data count check
        print("Data count matched !!!")
        print("Data count checked and confirmed !!!")
    else:
        print("Data count mismatched !!!")
        raise AirflowException('Counts Mismatch')
                                                                        # datatypes check
    transformation_cols = config_json[transformed_data]['transformation-cols']
    
    if file_name == "actives":
        transformation_cols = {'user_latitude':float,'user_longitude':float,'location_source':str}
    elif file_name == "viewership":
        transformation_cols = {'user_lat':float,'user_long':float,'location_source':str}

    type_validation_check = True
    for column, col_type in transformation_cols.items():
        if isinstance(staging_df[column][0], col_type):
            print(f"Datatype is valid for column {column}")
        else:
            print(f"Datatype is invalid for column {column}")
            type_validation_check = False

    if type_validation_check:
        print("Data type checked and confirmed !!!")
        print("All Post validation and Pre validation checks completed !!!")
    else:
        raise Exception("Post Validation Failed !!!")

def cluster_Termination(**kwargs):
    """
    Terminates a cluster. This terminates all instances in the cluster and cannot
    be undone. Any data not saved elsewhere, such as in an Amazon S3 bucket, is lost.
    """
    try:
        cluster_id = kwargs['ti'].xcom_pull(task_ids = "EMR_cluster_creation")
        spark_job_status = kwargs['ti'].xcom_pull(task_ids = "spark_job_final_status")
        if spark_job_status == "success" or spark_job_status == "dead":
            print("Terminated cluster %s.", cluster_id)
            emr.terminate_job_flows(JobFlowIds = [cluster_id])
    except ClientError as ce:
        print("Couldn't terminate cluster", cluster_id)
        emr.terminate_job_flows(JobFlowIds = [cluster_id])

# Initializing the DAG:

dag = DAG(
    dag_id = "Star-Project-Dag",
    start_date = datetime.now(),
    concurrency = 3,
    schedule_interval = None,
    default_args = default_args,
    description = "Star-Project-MileStone-8",
    catchup = False
    )

#---------------------------------------------------------------------------------------------------------------------------------------------
# Tasks / Operators

get_config = PythonOperator(
    task_id = "read_configurations",
    python_callable = get_Config,
    dag = dag
)

copy_data_landing_to_raw_zone = PythonOperator(
    task_id = "copy_data_landing_to_raw_zone",
    python_callable = copy_Data,
    dag = dag
)

pre_validation = PythonOperator(
    task_id = "pre_validation",
    python_callable = pre_Validation,
    dag = dag
)

EMR_Cluster_Creation = PythonOperator(
    task_id = "EMR_cluster_creation",
    python_callable = create_Emr_Cluster,
    dag = dag
)

cluster_ready_ackn = PythonOperator(
    task_id = "cluster_ready_acknowledge",
    python_callable = cluster_Ready_Acknowledge
)

get_Cluster_DNS = PythonOperator(
    task_id = "get_Cluster_DNS",
    python_callable = get_ClusterDNS,
    dag = dag
)

submit_Livy_Spark = PythonOperator(
    task_id = 'submit_spark_through_livy',
    python_callable = submit_livy,
    dag = dag
)

spark_job_status = PythonOperator(
    task_id = 'spark_job_final_status',
    python_callable = livy_spark_job_status,
    dag = dag
)

post_validation = PythonOperator(
    task_id = "post_validation",
    python_callable = post_Validation,
    dag = dag
)

cluster_termination = PythonOperator(
    task_id = "cluster_termination",
    python_callable = cluster_Termination,
    trigger_rule = 'all_done',
    dag = dag
)

# Airflow --> workflow DAG
get_config >> copy_data_landing_to_raw_zone >> pre_validation >> EMR_Cluster_Creation >> cluster_ready_ackn >>  \
get_Cluster_DNS >> submit_Livy_Spark >> spark_job_status >> post_validation >> cluster_termination