from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, max as spark_max
import os
import psycopg2
import config as config  # Import the configuration

# Ensure the paths are correct
assert os.path.exists(config.JDBC_DRIVER), f"JDBC driver not found at {config.JDBC_DRIVER}"
assert os.path.exists(config.ICEBERG_SPARK_JAR), f"Iceberg Spark JAR not found at {config.ICEBERG_SPARK_JAR}"

# Initialize Spark session with Iceberg, Nessie, and Hadoop AWS configurations
spark = SparkSession.builder \
    .appName("PostgresToIceberg") \
    .config("spark.jars", f"{config.JDBC_DRIVER},{config.ICEBERG_SPARK_JAR}") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901,software.amazon.awssdk:s3:2.20.131,software.amazon.awssdk:sts:2.20.131,software.amazon.awssdk:kms:2.20.131,software.amazon.awssdk:glue:2.20.131,software.amazon.awssdk:dynamodb:2.20.131") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.catalog.warehouse", config.WAREHOUSE_PATH) \
    .config("spark.sql.catalog.catalog.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.catalog.uri", "http://localhost:19120/api/v1") \
    .config("spark.sql.catalog.catalog.ref", "main") \
    .config("spark.sql.catalog.catalog.cache-enabled", "false") \
    .config("spark.hadoop.fs.s3a.access.key", config.S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", config.S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.hadoop.fs.s3a.region", config.AWS_REGION) \
    .config("spark.sql.catalog.catalog.aws.region", config.AWS_REGION) \
    .getOrCreate()

# Function to create schema if not exists
def create_schema_if_not_exists(schema):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"Schema {schema} created or already exists.")
    except Exception as e:
        print(f"Error creating schema {schema}: {e}")

# Function to get list of tables from the schema
def get_tables_from_schema(schema):
    conn = psycopg2.connect(host=config.DB_URL.split('/')[2].split(':')[0], port=config.DB_URL.split('/')[2].split(':')[1], dbname=config.DB_NAME, user=config.DB_USER, password=config.DB_PASSWORD)
    cur = conn.cursor()
    cur.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema}'")
    tables = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return tables

# Function to get the latest timestamp from the Iceberg table
def get_latest_timestamp(table_name):
    try:
        df = spark.read.table(f"catalog.bronze.{table_name}")
        latest_timestamp = df.agg(spark_max("ingestion_timestamp")).collect()[0][0]
        return latest_timestamp
    except:
        return None

# Function to create an Iceberg table if it does not exist
def create_iceberg_table_if_not_exists(df, table_name):
    try:
        spark.read.table(f"catalog.bronze.{table_name}")
    except:
        df.writeTo(f"catalog.bronze.{table_name}").create()
        print(f"Created new Iceberg table {table_name} at {config.WAREHOUSE_PATH}/bronze/{table_name}")

# Function to load table from PostgreSQL and write to Iceberg incrementally
def load_and_write_table_incremental(table_name):
    latest_timestamp = get_latest_timestamp(table_name)
    
    if latest_timestamp:
        query = f"(SELECT * FROM {config.DB_SCHEMA}.{table_name} WHERE updated_at > '{latest_timestamp}') AS temp"
    else:
        query = f"(SELECT * FROM {config.DB_SCHEMA}.{table_name}) AS temp"
    
    df = spark.read.format("jdbc") \
        .option("url", config.DB_URL) \
        .option("dbtable", query) \
        .option("user", config.DB_USER) \
        .option("password", config.DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()
    
    if df.count() == 0:
        print(f"No new data to write for {table_name}.")
        return
    
    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    # Create the Iceberg table if it does not exist
    create_iceberg_table_if_not_exists(df, table_name)
    
    # Append data to the Iceberg table
    df.writeTo(f"catalog.bronze.{table_name}").append()
    print(f'Successfully written {table_name} to Iceberg at {config.WAREHOUSE_PATH}/bronze/{table_name}')

# Create schema if it does not exist
create_schema_if_not_exists("catalog.bronze")

# Get list of tables from the schema
tables = get_tables_from_schema(config.DB_SCHEMA)

# Load each table and write to Iceberg incrementally
for table in tables:
    load_and_write_table_incremental(table)

# Stop the Spark session
spark.stop()
