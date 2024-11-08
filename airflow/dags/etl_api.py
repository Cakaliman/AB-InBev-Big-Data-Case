from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import requests
import json
import logging
import os


# Define your API URL and S3 bucket parameters
API_URL = 'https://api.openbrewerydb.org/v1/breweries/{obdb-id}'

S3_BUCKET = "bucket-name"
S3_KEY = "hash-string"


# Initialize Spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("MedallionArchitecture") \
        .getOrCreate()

# Capture the amount of braweries (API) | "Web-scrapper"
def list_braweries(api_url):
    brawery_list = []
    # Get API request
    response = requests.get()
    try:
        if response.status_code == 200:
            data = response.json()  # Parse JSON response
            for brewery in data:
                brawery_list = brewery['name'].append()
            return brawery_list

    except Exception as e:
        logging.error("Failed to retrieve data:", response.status_code)

def failure_alert(context):
    return SlackWebhookOperator(
        task_id     ='slack_alert',
        http_conn_id='slack_default',  # Connection ID for Slack webhook
        message     =f"Task {context['task_instance'].task_id} in DAG {context['task_instance'].dag_id} failed",
        username    ='airflow'
    ).execute(context=context)


def generate_dag(dag_id, start_date, schedule_interval):
    
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####
    ########################### ########################### ########################### ########################### ########################### ###########################
    ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ### ### ### ###BEGIN### ### ###
    ########################### ########################### ########################### ########################### ########################### ###########################
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####


    tags = ['ETL', 'API', 'ELT', 'AB InBev', 'Incremental']

    # Define default arguments for the DAG considering the failure alert
    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': True,
        'on_failure_callback': failure_alert,
        'email': ['cacaliman.santos@gmail.com'],
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
        @task(retries = 0)
        def init(**kwargs):

            # Define 'ti' for XCOM usage
            ti = kw["ti"]

            # USING XCOM_PUSH TO REQUEST DATA
            ti.xcom_push(key='start_date',          value = str(start_date))
            ti.xcom_push(key='api_url',             value = API_URL)
            ti.xcom_push(key='s3_bucket',           value = S3_BUCKET)
            ti.xcom_push(key='s3_key',              value = S3_KEY)

        # Function to extract data from the API
        @task(retries = 0)
        def extract_data_from_api(**kwargs):
            ti      = kwargs["ti"]
            api_url = ti.xcom_pull(task_ids = 'init', key = 'api_key')

            try:
                all_data = []  # To store all the data

                page = 1
                while True:
                    # Construct the URL for the current page
                    url = f"{api_url}?page={page}&per_page=50"  # Adjust per_page as needed
                    response = requests.get(url)
                    
                    # Check if the request was successful
                    if response.status_code != 200:
                        print("Error retrieving data:", response.status_code)
                        break

                    # Parse the JSON response
                    data = response.json()

                    # Break the loop if there's no data left
                    if not data:
                        break

                    # Append the data from this page to the main list
                    all_data.extend(data)
                    page += 1  # Move to the next page

                # Save all the data to a single JSON file
                with open("/tmp/all_breweries_data.json", "w") as file:
                    json.dump(all_data, file, indent=4)  # indent=4 for pretty formatting

            except requests.exceptions.RequestException as e:
                logging.error("Failed to fetch data from API: %s", e)
                raise  # Re-raise the exception to mark the task as failed

        # Function to ingest data to load it into an S3 bucket
        @task(retries = 0)
        def ingest_task_to_s3(**kwargs):
            ti             = kwargs["ti"]
            S3_KEY         = ti.xcom_pull(task_ids = 'init',                   key = 's3_key')    
            S3_BUCKET_NAME = ti.xcom_pull(task_ids = 'init',                   key = 's3_bucket')
            DATA_FILE_PATH = "/tmp/all_breweries_data.json"
            
            try:
                s3_hook = S3Hook(aws_conn_id='aws_default')
                # Upload file to S3
                s3_hook.load_file(
                    filename=DATA_FILE_PATH,
                    key=S3_KEY,
                    bucket_name=S3_BUCKET_NAME,
                    replace=True
                )
                logging.info(f"File successfully uploaded to S3 bucket '{S3_BUCKET_NAME}' with key '{S3_KEY}'")
            except Exception as e:
                logging.error(f"Failed to upload file to S3: {e}")
                raise  # Re-raise the exception to mark the task as failed

        # Trigger glue function 
        @task(retries = 0)
        def trigger_glue(**kw):
            ti     = kw['ti']
            key    = ti.xcom_pull(task_ids='init', key='key')
            secret = ti.xcom_pull(task_ids='init', key='secret')
            region = ti.xcom_pull(task_ids='init', key='region')

            glue = Glue( decrypt(key), decrypt(secret), region )
            glue.start_job('elt_api')
            logging.info(f'{glue.job_id = }')
            ti.xcom_push('run_id', glue.job_id)

        # Check the glue status if its done or not
        @task(retries = 0)
        def check_glue_job_status(**kw):
            ti      = kw['ti']
            run_id  = ti.xcom_pull(task_ids='trigger_glue', key='run_id') 
            key     = ti.xcom_pull(task_ids='init',         key='key')
            secret  = ti.xcom_pull(task_ids='init',         key='secret')
            region  = ti.xcom_pull(task_ids='init',         key='region')

            logging.info(f"{type(ts) = }")
            logging.info(f"{run_id = }")

            glue    = Glue(decrypt(key), decrypt(secret), region)

            while glue.job_status not in ['SUCCEEDED']:
                glue.get_job('etl_api', run_id)
                logging.info(f"{glue.job_status = }")
                if glue.job_status in ['STOPPED', 'TIMEOUT', 'FAILED']:
                    raise AirflowException
                time.sleep(10)     

        # Clean data from local file
        @task()
        def clean_up(**kwargs):
            tmp_log     = os.path.isfile(f'/tmp/all_breweries_data.json')
            # in case the file exists, it will execute
            if tmp_log == True:
                os.remove(f'/tmp/all_breweries_data.json')

        # Set up task dependencies
        clean_up() >> init() >> extract_data_from_api() >> ingest_task_to_s3() >> trigger_glue() >> check_glue_job_status()

    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####
    ########################### ########################### ########################### ########################### ########################### ###########################
    ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ### ### ### ### END ### ### ###
    ########################### ########################### ########################### ########################### ########################### ###########################
    #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   ##### #####   ###########   #####

    return dag



##############################################################################################


dag_id     = f"brawery_api_integration"
start_date = datetime(2022,1,1, 13, 00)

generate_dag(dag_id, start_date, '@daily')


##############################################################################################





# bY cAKalimAN
