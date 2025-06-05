import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import pymysql


# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'source_bucket',
    'source_prefix',
    'mysql_database', 
    'mysql_table'
])

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define source and target
source_path = f"s3://{args['source_bucket']}/{args['source_prefix']}"
connection_name = "mysql-connection"
mysql_database = args['mysql_database']
mysql_table = args['mysql_table']

# Read from S3
try:
    datasource = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={
            "paths": [source_path]
            },
        format="csv",
        format_options={
            "withHeader": True,
            "separator": ","
        }
    )
except Exception as e:
    print(f"Failed to read from S3 path: {source_path}")
    print("Error details:")
    traceback.print_exc()
    sys.exit(1)

# Apply transformations if needed
# For example:
# datasource = ApplyMapping.apply(
#     frame=datasource,
#     mappings=[
#         ("col1", "string", "target_col1", "string"),
#         ("col2", "int", "target_col2", "int"),
#         # Add more column mappings as needed
#     ]
# )

# Convert Glue DynamicFrame to Spark DataFrame
df = datasource.toDF()

# Collect rows (only for small datasets!)
rows = df.collect()

# MySQL connection
conn = pymysql.connect(
    host='your-rds-endpoint',
    user='your_user',
    password='your_password',
    db='your_db'
)
cursor = conn.cursor()

# Your upsert query
insert_query = """
INSERT INTO Sales.Test (Number, Name, Category)
VALUES (%s, %s, %s)
ON DUPLICATE KEY UPDATE Name = VALUES(Name), Category = VALUES(Category)
"""

# Loop over rows and execute upserts
for row in rows:
    cursor.execute(insert_query, (row['Number'], row['Name'], row['Category']))

conn.commit()
cursor.close()
conn.close()

