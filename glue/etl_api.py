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
bronze_path = "s3://your-bucket/raw/"
silver_path = "s3://your-bucket/silver/"
gold_path = "s3://your-bucket/gold/"

# Bronze Layer - Load Raw Data
bronze_df = spark.read.json(bronze_path)  # Example for JSON; adjust format if needed
bronze_df.write.mode("overwrite").parquet(f"{bronze_path}/bronze_table/")

# Silver Layer - Transform Data (Cleansing and Processing)
# Example: Filtering null values, removing duplicates
silver_df = bronze_df \
    .filter(col("important_field").isNotNull()) \
    .dropDuplicates(["unique_key_column"])

# Enriching data - Add derived columns or transformations
silver_df = silver_df.withColumn("status", when(col("value") > 100, "High").otherwise("Low"))

# Write Silver Layer Data
silver_df.write.mode("overwrite").parquet(f"{silver_path}/silver_table/")

# Gold Layer - Aggregate Data
# Example: Aggregating data for analysis, preparing final refined table
gold_df = silver_df.groupBy("category_column").agg(
    {"value": "avg", "other_metric": "sum"}
)

# Write Gold Layer Data
gold_df.write.mode("overwrite").parquet(f"{gold_path}/gold_table/")

# Commit the job
job.commit()

