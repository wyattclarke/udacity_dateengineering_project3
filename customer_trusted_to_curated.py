import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1687222196463 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted_node1687222196463",
)

# Script generated for node customer_trusted
customer_trusted_node1687222077696 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1687222077696",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1687222287685 = DynamicFrame.fromDF(
    accelerometer_trusted_node1687222196463.toDF().dropDuplicates(["user"]),
    glueContext,
    "DropDuplicates_node1687222287685",
)

# Script generated for node Join
Join_node1687222218159 = Join.apply(
    frame1=customer_trusted_node1687222077696,
    frame2=DropDuplicates_node1687222287685,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1687222218159",
)

# Script generated for node Drop Fields
DropFields_node1687222661363 = DropFields.apply(
    frame=Join_node1687222218159,
    paths=["y", "x", "user", "z", "timeStamp"],
    transformation_ctx="DropFields_node1687222661363",
)

# Script generated for node customer_curated
customer_curated_node1687222382731 = glueContext.getSink(
    path="s3://wyatt-7654/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_node1687222382731",
)
customer_curated_node1687222382731.setCatalogInfo(
    catalogDatabase="wyatt-udacity-db", catalogTableName="customer_curated"
)
customer_curated_node1687222382731.setFormat("json")
customer_curated_node1687222382731.writeFrame(DropFields_node1687222661363)
job.commit()
