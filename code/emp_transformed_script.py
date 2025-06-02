import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_date, lit, current_date, datediff
from datetime import datetime
import boto3
from pyspark.sql.functions import year, month, dayofmonth

# Get job parameters
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get S3 path from Catalog
glue_client = boto3.client('glue', region_name='ap-south-1')
response = glue_client.get_table(DatabaseName='processed-database', Name='process_emp')
s3_location = response['Table']['StorageDescriptor']['Location']  # e.g., s3://dq-checks-glue-dev/raw-file/emp/
print(s3_location)

# Read data from S3
source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [s3_location],
        "recurse": True,
        "partitionKeys": ["partition_0"]
    },
    format="parquet",
    transformation_ctx="source_data"
)

source_df = source_dynamic_frame.toDF()

if source_df.rdd.isEmpty():
    glueContext.get_logger().info("No new data to process (bookmarks skipped all files). Exiting gracefully.")
else:
    # Filter valid rows
    valid_df = source_df.filter(
        (col("emp_name_null_check_flag") == 1) &
        (col("emp_name_char_check_flag") == 1) &
        (col("dob_date_check_flag") == 1)
    )

    columns = valid_df.columns
    salary_col = col("salary").cast("double") if "salary" in columns else lit(None).cast("double")

    # Transform valid records
    transformed_df = valid_df.select(
        col("emp_id").cast("string").alias("employee_id"),
        col("emp_name").alias("employee_name"),
        to_date(col("dob"), "yyyy-MM-dd").alias("date_of_birth"),
        salary_col.alias("salary"),
        lit(current_date()).alias("processed_date"),
        (datediff(current_date(), to_date(col("dob"), "yyyy-MM-dd")) / 365.25).cast("integer").alias("age"),
        year(current_date()).alias("year"),
        month(current_date()).alias("month"),
        dayofmonth(current_date()).alias("day")
    )

    # Convert to DynamicFrame
    transformed_dynamic_frame = DynamicFrame.fromDF(transformed_df, glueContext, "processed_frame")

    # Write to curated path partitioned by year/month/day
    current_date_str = datetime.now().strftime("%Y-%m-%d")
    curated_path = f"s3://dq-checks-glue-dev/transformed/emp/"

    glueContext.write_dynamic_frame.from_options(
        frame=transformed_dynamic_frame,
        connection_type="s3",
        format="parquet",
        connection_options={"path": curated_path, "partitionKeys": ["year", "month", "day"]},
        transformation_ctx="transformed_data"
    )

job.commit()
