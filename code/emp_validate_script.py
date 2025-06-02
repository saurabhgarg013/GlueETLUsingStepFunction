import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from datautilty import check_null, check_special_characters, check_date_format
from datetime import datetime
import boto3

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get S3 path from Catalog
glue_client = boto3.client('glue', region_name='ap-south-1')
response = glue_client.get_table(DatabaseName='raw-database', Name='raw_emp')
s3_location = response['Table']['StorageDescriptor']['Location']  # e.g., s3://dq-checks-glue-dev/raw-file/emp/
print(s3_location)


# # Read data from S3 with transformation_ctx
source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [s3_location],
        "recurse": True,
        "partitionKeys": ["partition_0"]  # Matches crawler’s partition key
    },
    format="csv",
    format_options={
        "withHeader": True,
         "separator": "," 
    },
    transformation_ctx="source_data"  # Unique context for S3 read
)

# Log sample files and schema
source_df = source_dynamic_frame.toDF()

# Check if DataFrame is empty
if source_df.rdd.isEmpty():
    glueContext.get_logger().info("No new data to process (bookmarks skipped all files). Exiting gracefully.")
      # Commit to save bookmark state

else:   

    # Apply quality checks
    processed_df = check_null(source_df, "emp_name")
    processed_df = check_special_characters(processed_df, "emp_name", "^[a-zA-Z ]+$")
    processed_df = check_date_format(processed_df, "dob", "yyyy-MM-dd")
    
    # Convert to DynamicFrame
    processed_dynamic_frame = DynamicFrame.fromDF(processed_df, glueContext, "processed_frame")
    
    # Write output to S3 with transformation_ctx
    current_date_time = datetime.now().strftime("%Y%m%d%H%M%S")
    output_path = f"s3://dq-checks-glue-dev/processed/emp/{current_date_time}/"
    glueContext.write_dynamic_frame.from_options(
        frame=processed_dynamic_frame,
        connection_type="s3",
        format="parquet",
        connection_options={"path": output_path, "partitionKeys": []},
        transformation_ctx="output_data"  # Unique context for S3 write
    )

# Commit job to save bookmarks
job.commit()