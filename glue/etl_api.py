import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when

# Initialize Glue job context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define S3 paths for Bronze, Silver, and Gold layers
S3_BUCKET = "bucket-name"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/"

# Bronze Layer - Load Raw Data
bronze_df = spark.read.json(bronze_path)  # Example for JSON; adjust format if needed
bronze_df.write.format("delta").mode("overwrite").save(f"{bronze_path}/bronze/")

# Silver Layer - Transform Data (Cleansing and Processing)
# Example: Filtering null values, removing duplicates
silver_df = bronze_df \
    .filter(col("important_field").isNotNull()) \
    .dropDuplicates(["unique_key_column"])

# Enriching data - Add derived columns or transformations
silver_df = silver_df.withColumn("status", \ 
    when(col("value") > 100, "High").otherwise("Low"))

# Write Silver Layer Data
silver_df.write.format("delta").mode("overwrite").save(f"{silver_path}/silver/")

# Gold Layer - Aggregate Data
# Example: Aggregating data for analysis, preparing final refined table
gold_df = silver_df.groupBy("category_column").agg(
    {"value": "avg", "other_metric": "sum"}
)

# Write Gold Layer Data
gold_df.write.format("delta").mode("overwrite").save(f"{gold_path}/gold/")

# Commit the job
job.commit()

