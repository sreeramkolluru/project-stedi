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
accelerometer_landing_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_landing_node1",
)

# Script generated for node customer_trusted
customer_trusted_node1690782711117 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="stedi_customer_trusted",
    transformation_ctx="customer_trusted_node1690782711117",
)

# Script generated for node Join
Join_node1690782730213 = Join.apply(
    frame1=accelerometer_landing_node1,
    frame2=customer_trusted_node1690782711117,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1690782730213",
)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node3 = glueContext.getSink(
    path="s3://project-stedi/accelerometer-trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="accelerometer_trusted_node3",
)
accelerometer_trusted_node3.setCatalogInfo(
    catalogDatabase="glue-database", catalogTableName="stedi_accelerometer_trusted"
)
accelerometer_trusted_node3.setFormat("json")
accelerometer_trusted_node3.writeFrame(Join_node1690782730213)
job.commit()
