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

# Script generated for node accelerometer_landing
accelerometer_landing_node1687219635055 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1687219635055",
)

# Script generated for node customer_trusted
customer_trusted_node1687219526505 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://wyatt-7654/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted_node1687219526505",
)

# Script generated for node filter_accelerometers
filter_accelerometers_node1687219736269 = Join.apply(
    frame1=customer_trusted_node1687219526505,
    frame2=accelerometer_landing_node1687219635055,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="filter_accelerometers_node1687219736269",
)

# Script generated for node Drop Fields
DropFields_node1687219932352 = DropFields.apply(
    frame=filter_accelerometers_node1687219736269,
    paths=[
        "birthday",
        "registrationdate",
        "sharewithresearchasofdate",
        "customername",
        "email",
        "lastupdatedate",
        "phone",
        "sharewithfriendsasofdate",
        "serialnumber",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1687219932352",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1687219802604 = glueContext.getSink(
    path="s3://wyatt-7654/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node1687219802604",
)
accelerometer_trusted_node1687219802604.setCatalogInfo(
    catalogDatabase="wyatt-udacity-db", catalogTableName="accelerometer_trusted"
)
accelerometer_trusted_node1687219802604.setFormat("json")
accelerometer_trusted_node1687219802604.writeFrame(DropFields_node1687219932352)
job.commit()
