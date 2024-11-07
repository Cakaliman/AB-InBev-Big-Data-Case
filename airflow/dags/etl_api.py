from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import requests
import json
import logging

# Define your API URL and S3 bucket parameters
API_URL = 'https://your-api-endpoint.com/data'
S3_BUCKET = 'your-s3-bucket-name'
S3_KEY = 'landing/data.json'  # The path where the data will be stored in S3

LANDING_AREA = f"s3://{S3_BUCKET}/bronze/" 
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"


# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Initialize Spark session
def create_spark_session():
    return SparkSession.builder \
        .appName("MedallionArchitecture") \
        .getOrCreate()




def generate_dag(dag_id, start_date, schedule_interval, prof):
    
    ### ### ### ### ### ### ###
    ### ### ### BEGIN ### ### ###
    ### ### ### ### ### ### ###

    """
        ETL API data 

    """

    with DAG(
        'etl_api',
        default_args=default_args,
        description='A DAG to extract data from an API and store it in S3 with error handling',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 1),
        catchup=False,
        ) as dag:



        # Function to extract data from the API
        @task()
        def extract_data_from_api(**kwargs):
            try:
                response = requests.get(API_URL)
                response.raise_for_status()  # Raise an error if the API call fails
                data = response.json()       # Assuming the API returns JSON data
                return data

            except requests.exceptions.RequestException as e:
                logging.error(f"API request failed: {e}")
                raise
            except ValueError as e:
                logging.error(f"Failed to parse API response as JSON: {e}")
                raise

        # Function to ingest data into S3
        @task()
        def ingest_data_to_s3(**kwargs):
            try:
                # Fetch data from the previous task
                ti      = kwargs['ti']
                data    = ti.xcom_pull(task_ids='extract_data_from_api')


                
                if data is None:
                    raise ValueError("No data retrieved from API extraction task")

                # Convert data to JSON and upload to S3
                s3 = S3Hook(aws_conn_id='your_s3_connection')
                s3.load_string(
                    string_data=json.dumps(data),
                    key=S3_KEY,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                logging.info(f"Data successfully ingested to s3://{S3_BUCKET}/{S3_KEY}")

            except ValueError as e:
                logging.error(f"Data validation error: {e}")
                raise
            except Exception as e:
                logging.error(f"Failed to ingest data into S3: {e}")
                raise



        # Bronze Layer: Load raw data into Bronze
        def bronze_layer(**kwargs):
            spark = create_spark_session()
            try:
                # Read raw data from the source (e.g., API or external S3 location)
                raw_data = spark.read.json("s3://your-source-bucket/raw-data/")
                
                # Save raw data to Bronze layer in S3
                raw_data.write.mode("overwrite").parquet(BRONZE_PATH)
                logging.info(f"Data successfully ingested to Bronze layer at {BRONZE_PATH}")
                
            except Exception as e:
                logging.error(f"Error in Bronze layer processing: {e}")
                raise
            finally:
                spark.stop()

        # Silver Layer: Transform Bronze data into Silver (e.g., filtering, cleansing)
        def silver_layer(**kwargs):
            spark = create_spark_session()
            try:
                # Read Bronze data
                bronze_data = spark.read.parquet(BRONZE_PATH)
                
                # Apply transformations (e.g., filtering, type conversions)
                silver_data = bronze_data.filter("column_name IS NOT NULL")
                
                # Save transformed data to Silver layer in S3
                silver_data.write.mode("overwrite").parquet(SILVER_PATH)
                logging.info(f"Data successfully transformed to Silver layer at {SILVER_PATH}")
                
            except Exception as e:
                logging.error(f"Error in Silver layer processing: {e}")
                raise
            finally:
                spark.stop()

        # Gold Layer: Aggregate Silver data into Gold
        def gold_layer(**kwargs):
            spark = create_spark_session()
            try:
                # Read Silver data
                silver_data = spark.read.parquet(SILVER_PATH)
                
                # Perform aggregations (e.g., grouping, summarizing)
                gold_data = silver_data.groupBy("group_column").agg({"value_column": "sum"})
                
                # Save final data to Gold layer in S3
                gold_data.write.mode("overwrite").parquet(GOLD_PATH)
                logging.info(f"Data successfully transformed to Gold layer at {GOLD_PATH}")
                
            except Exception as e:
                logging.error(f"Error in Gold layer processing: {e}")
                raise
            finally:
                spark.stop()



        # Set up task dependencies
        extract_task() >> ingest_task() >> bronze_transformation() >> silver_transformation() >> gold_transformation()

    
    ### ### ### ### ### ### ###
    ### ### ### END ### ### ###
    ### ### ### ### ### ### ###

    return dag


# Instantiate the DAG

generate_dag()