from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import logging
import os


# Define your API URL and S3 bucket parameters
API_URL = 'https://api.openbrewerydb.org/v1/breweries/{obdb-id}'

S3_BUCKET = "bucket-name"
S3_KEY = "hash-string"

LANDING_AREA = f"s3://{S3_BUCKET}/bronze/" 
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"

# Initialize Spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("MedallionArchitecture") \
        .getOrCreate()

# Capture the amount of braweries (API)
def list_braweries(api_url):

    # Get API request
    response = requests.get()

    brawery_list = []
    try:
        if response.status_code == 200:
            data = response.json()  # Parse JSON response
            for brewery in data:
                brawery_list = brewery['name'].append()
            return brawery_list

    except Exception as e:
        logging.error("Failed to retrieve data:", response.status_code)



def generate_dag(dag_id, start_date, schedule_interval, brewery):
    
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####
    ########################### ########################### ########################### ########################### ########################### ###########################
    ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ###
    ########################### ########################### ########################### ########################### ########################### ###########################
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####


    tags = ['ETL', 'API', 'ELT', 'AB InBev', 'Incremental']

    # Define default arguments for the DAG
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


    # Schedules the DAG daily (one excecution per day)
    end_date            = kw['dag_run'].execution_date
    start_date          = end_date - timedelta(days=1)
    year, month, day    = str(start_date)[0:10].split('-')

    logging.info(f"{year = }, {month = }, {day = }")



    with DAG(
        'etl_api',
        default_args=default_args,
        description='Incremental DAG to extract data from an API and transform it into a aggregated vision',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
    ) as dag:

        # Declare all XCOM variables 
        @task()
        def init(**kwargs):

            # Define 'ti' for XCOM usage
            ti = kw["ti"]

            # USING XCOM_PUSH TO REQUEST DATA
            ti.xcom_push(key='start_date',          value = str(start_date))
            ti.xcom_push(key='api_url',             value = API_URL)
            ti.xcom_push(key='s3_bucket',           value = API_URL)
            ti.xcom_push(key='s3_key',              value = API_URL)
            ti.xcom_push(key='landing_area',        value = LANDING_AREA)
            ti.xcom_push(key='bronze_path',         value = BRONZE_PATH)
            ti.xcom_push(key='silver_path',         value = SILVER_PATH)
            ti.xcom_push(key='gold_path',           value = GOLD_PATH)


        # Function to extract data from the API
        @task()
        def extract_data_from_api(**kwargs):
            ti      = kwargs["ti"]
            api_url = ti.xcom_pull(task_ids = 'init', key = 'api_key')

            try:
                response = requests.get(API_URL)
                response.raise_for_status()  # Raise an error for bad status codes
                data = response.json()
                
                # Store the data in a temporary file
                with open(DATA_FILE_PATH, 'w') as f:
                    json.dump(data, f)
                logging.info("Data successfully fetched and stored in %s", DATA_FILE_PATH)
            except requests.exceptions.RequestException as e:
                logging.error("Failed to fetch data from API: %s", e)
                raise  # Re-raise the exception to mark the task as failed

        # Function to ingest data to load it into an S3 bucket
        @task()
        def ingest_task_to_s3(**kwargs):
            ti      = kwargs["ti"]
            api_url = ti.xcom_pull(task_ids = 'init', key = 's3_')

            try:
                s3_hook = S3Hook(aws_conn_id='aws_default')
                
                # Upload file to S3
                s3_hook.load_file(
                    filename=DATA_FILE_PATH,
                    key=S3_KEY,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True
                )
                logging.info("File successfully uploaded to S3 bucket '%s' with key '%s'", S3_BUCKET_NAME, S3_KEY)
            except Exception as e:
                logging.error("Failed to upload file to S3: %s", e)
                raise  # Re-raise the exception to mark the task as failed
      @task(retries = 0)
        def trigger_glue(**kw):
            ti          = kw['ti']

            key    = ti.xcom_pull(task_ids='init', key='key')
            secret = ti.xcom_pull(task_ids='init', key='secret')
            region = ti.xcom_pull(task_ids='init', key='region')

            glue = Glue( decrypt(key), decrypt(secret), region )
            glue.start_job('transform_job')
            logging.info(f'{glue.job_id = }')
            ti.xcom_push('run_id', glue.job_id)

        @task(retries = 0)
        def check_glue_job_status(**kw):
            
            ti      = kw['ti']
            ts      = kw['ts']
            run_id  = ti.xcom_pull(task_ids='trigger_glue', key='run_id') 
            key     = ti.xcom_pull(task_ids='init', key='key')
            secret  = ti.xcom_pull(task_ids='init', key='secret')
            region  = ti.xcom_pull(task_ids='init', key='region')

            logging.info(f"{ts = }")
            logging.info(f"{type(ts) = }")
            logging.info(f"{run_id = }")

            glue    = Glue(decrypt(key), decrypt(secret), region)

            while glue.job_status not in ['SUCCEEDED']:
                glue.get_job('transform_job', run_id)
                logging.info(f"{glue.job_status = }")
                if glue.job_status in ['STOPPED', 'TIMEOUT', 'FAILED']:
                    raise AirflowException
                time.sleep(10)     
            

        # Set up task dependencies
        extract_task() >> ingest_task_to_s3() >> bronze_transformation() >> silver_transformation() >> gold_transformation()

    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####
    ########################### ########################### ########################### ########################### ########################### ###########################
    ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ###
    ########################### ########################### ########################### ########################### ########################### ###########################
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####

    return dag



##############################################################################################


list_brawery = []
list_brawery = list_braweries(api_url)

for brawery in list_brawery:
    """
        For each brawery create a new dag
    """


    dag_name           = ''.join( word.title() for word in brawery.name.split('-'))
    dag_id             = f"brawery_{dag_name}_integration"
    start_date         = brawery.airflow_start
    
    if start_date == None:  start_date = datetime(2022,6,30, 13, 00)
    else:                   start_date = datetime(brawery.airflow_start.year, brawery.airflow_start.month, brawery.airflow_start.day)
    
    globals()[dag_id] = generate_dag(dag_id, start_date, '@daily', brawery)



##############################################################################################

# bY cAKalimAN




