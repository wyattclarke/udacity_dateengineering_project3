import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1687213546806 = glueContext.create_dynamic_frame.from_catalog(
    database="wyatt-udacity-db",
    table_name="customer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1687213546806",
)

# Script generated for node Filter
Filter_node1687213586018 = Filter.apply(
    frame=AWSGlueDataCatalog_node1687213546806,
    f=lambda row: (row["sharewithresearchasofdate"] == 1655293787680),
    transformation_ctx="Filter_node1687213586018",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://wyatt-7654/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="wyatt-udacity-db", catalogTableName="customer_trusted_test"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(Filter_node1687213586018)
job.commit()
