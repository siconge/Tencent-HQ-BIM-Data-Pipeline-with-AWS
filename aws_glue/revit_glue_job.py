import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_nodexxxxxxxxxxxxx = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": "\"",
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://tencent-headquarters-bim/raw/"],
        "recurse": True
    },
    transformation_ctx="AmazonS3_nodexxxxxxxxxxxxx")

# Convert DynamicFrame to DataFrame
df = AmazonS3_nodexxxxxxxxxxxxx.toDF()

# DataFrame Transformation: Concatenate the specified columns into a single column
from pyspark.sql.functions import concat_ws
result_df = df.withColumn('louver_rescue_case', concat_ws('-', df['ventilation_louver'], df['rescue_window'])).drop('ventilation_louver', 'rescue_window')

# Convert DataFrame back to DynamicFrame
from awsglue import DynamicFrame
resultDynamicFrame = DynamicFrame.fromDF(result_df, glueContext, 'resultDynamicFrame')

# Script generated for node Amazon S3
AmazonS3_nodexxxxxxxxxxxxx = glueContext.write_dynamic_frame.from_options(
    frame=resultDynamicFrame,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://tencent-headquarters-bim/transformed/",
        "partitionKeys": []
    },
    transformation_ctx="AmazonS3_nodexxxxxxxxxxxxx"
)

job.commit()
