from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Replace with your GCS input and BigQuery output details
INPUT_GCS_PATH = "gs://us-central1-sumanth1-750c33f0-bucket/dags/input_data.csv"
BQ_OUTPUT_DATASET = "Company"
BQ_OUTPUT_TABLE = "employees"
BQ_PROJECT_ID = "sss-3040efdcfdf64026be237121"  # Specify your GCP project ID

# Create a SparkSession
spark = SparkSession.builder \
    .appName("GCS to BigQuery") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.24.2") \
    .getOrCreate()

# Read data from GCS
df = spark.read.csv(INPUT_GCS_PATH, header=True, inferSchema=True)

# Create a temporary view to use Spark SQL
df.createOrReplaceTempView("employees")

# Perform SQL query to filter employees with salary more than 2000
filtered_df = spark.sql("SELECT * FROM employees WHERE SALARY > 2000")

# Write data to BigQuery
filtered_df.write.format("bigquery") \
    .option("table", f"{BQ_PROJECT_ID}.{BQ_OUTPUT_DATASET}.{BQ_OUTPUT_TABLE}") \
    .mode("overwrite") \
    .save()

# Stop the SparkSession
spark.stop()
