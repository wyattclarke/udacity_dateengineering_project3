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

# Script generated for node step_trainer_landing
step_trainer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="step_trainer_landing_node1",
)

# Script generated for node customer_curated
customer_curated_node1687226675804 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customer_curated_node1687226675804",
)

# Script generated for node Join
Join_node1687226713297 = Join.apply(
    frame1=step_trainer_landing_node1,
    frame2=customer_curated_node1687226675804,
    keys1=["serialNumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1687226713297",
)

# Script generated for node Drop Fields
DropFields_node1687227178185 = DropFields.apply(
    frame=Join_node1687226713297,
    paths=[
        "sharewithfriendsasofdate",
        "registrationdate",
        "customername",
        "birthday",
        "sharewithpublicasofdate",
        "lastupdatedate",
        "email",
        "serialnumber",
        "sharewithresearchasofdate",
        "phone",
    ],
    transformation_ctx="DropFields_node1687227178185",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.getSink(
    path="s3://wyatt-7654/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node3",
)
step_trainer_trusted_node3.setCatalogInfo(
    catalogDatabase="wyatt-udacity-db", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node3.setFormat("json")
step_trainer_trusted_node3.writeFrame(DropFields_node1687227178185)
job.commit()
