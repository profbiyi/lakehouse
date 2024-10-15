import os

# Base directory for the project
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# PostgreSQL connection details
DB_URL = ''
DB_USER = ''
DB_PASSWORD = ''
DB_SCHEMA = 'sample_schema'
JDBC_DRIVER = os.path.join(BASE_DIR, 'jars/postgresql-42.7.3.jar')
DB_NAME = ''

# Path to Iceberg Spark JAR in the working directory
ICEBERG_SPARK_JAR = os.path.join(BASE_DIR, 'jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar')

# S3 details
S3_BUCKET = ''
WAREHOUSE_PATH = f's3a://{S3_BUCKET}'
S3_ACCESS_KEY = ''
S3_SECRET_KEY = ''
AWS_REGION = ''
S3_ENDPOINT = 'https://s3.us-east-1.amazonaws.com'

# Set AWS credentials as environment variables
os.environ['AWS_ACCESS_KEY_ID'] = S3_ACCESS_KEY
os.environ['AWS_SECRET_ACCESS_KEY'] = S3_SECRET_KEY
os.environ['AWS_REGION'] = AWS_REGION
