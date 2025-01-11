import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql.functions import current_timestamp, date_format

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BucketName', 'DatabaseName'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['BucketName']
database_name = args['DatabaseName']

# Script generated for source node Amazon S3
AmazonS3_S3CsvSource = glueContext.create_dynamic_frame_from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{bucket_name}/raw/"]},
    format="csv",
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False},
    transformation_ctx="AmazonS3_S3CsvSource"
)

# Script generated for transform node Add Current Timestamp
df_with_timestamp = AmazonS3_S3CsvSource.toDF().withColumn("timestamp", date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))
AddCurrentTimestamp_DynamicTransform = DynamicFrame.fromDF(
    df_with_timestamp,
    glueContext,
    "AddCurrentTimestamp_DynamicTransform"
)

# Script generated for target node Amazon S3
AmazonS3_S3GlueParquetTarget = glueContext.getSink(
    connection_type="s3",
    path=f"s3://{bucket_name}/partitioned/",
    updateBehavior="UPDATE_IN_DATABASE",
    enableUpdateCatalog=True,
    partitionKeys=["timestamp"],
    transformation_ctx="AmazonS3_S3GlueParquetTarget"
)
AmazonS3_S3GlueParquetTarget.setCatalogInfo(catalogDatabase=database_name, catalogTableName="partitioned")
AmazonS3_S3GlueParquetTarget.setFormat("glueparquet", compression="snappy")
AmazonS3_S3GlueParquetTarget.writeFrame(AddCurrentTimestamp_DynamicTransform)
job.commit()