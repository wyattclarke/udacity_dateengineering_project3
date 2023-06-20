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

# Script generated for node step_trusted
step_trusted_node1687228421706 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="step_trusted_node1687228421706",
)

# Script generated for node acce_trusted
acce_trusted_node1687228446731 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="acce_trusted_node1687228446731",
)

# Script generated for node Join
Join_node1687227967563 = Join.apply(
    frame1=step_trusted_node1687228421706,
    frame2=acce_trusted_node1687228446731,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1687227967563",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node1687228557839 = glueContext.getSink(
    path="s3://wyatt-7654/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="machine_learning_curated_node1687228557839",
)
machine_learning_curated_node1687228557839.setCatalogInfo(
    catalogDatabase="wyatt-udacity-db", catalogTableName="machine_learning_curated"
)
machine_learning_curated_node1687228557839.setFormat("json")
machine_learning_curated_node1687228557839.writeFrame(Join_node1687227967563)
job.commit()
