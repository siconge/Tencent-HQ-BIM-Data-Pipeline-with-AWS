import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1908673532890 = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": '"', "withHeader": True, "separator": ","},
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://tencent-headquarters-bim-data-engineering/raw/revit_20231001_unitized_curtain_wall.csv"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1908673532890",
)

# Convert DynamicFrame to DataFrame
df = AmazonS3_node1908673532890.toDF()

# Concatenate specified columns into a single column
from pyspark.sql.functions import concat_ws
from awsglue import DynamicFrame

df_combined = df.withColumn('louver_rescue_case', concat_ws('-', df['ventilation_louver'], df['rescue_window']))
df_combined = df_combined.drop('ventilation_louver', 'rescue_window')

# Convert back to DynamicFrame
s3_node_combined = DynamicFrame.fromDF(df_combined, glueContext, 's3_node_combined')

# Script generated for node Amazon S3
AmazonS3_node1908673537221 = glueContext.create_dynamic_frame.from_options(
    frame=s3_node_combined,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://tencent-headquarters-bim-data-engineering/transformed/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1908673537221",
)

job.commit()