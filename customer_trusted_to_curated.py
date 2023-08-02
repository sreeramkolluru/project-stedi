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

# Script generated for node stedi_customer_trusted
stedi_customer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="stedi_customer_trusted",
    transformation_ctx="stedi_customer_trusted_node1",
)

# Script generated for node stedi_accelerometer_trusted
stedi_accelerometer_trusted_node1690811946241 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="glue-database",
        table_name="stedi_accelerometer_trusted",
        transformation_ctx="stedi_accelerometer_trusted_node1690811946241",
    )
)

# Script generated for node Join
Join_node1690812241694 = Join.apply(
    frame1=stedi_customer_trusted_node1,
    frame2=stedi_accelerometer_trusted_node1690811946241,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1690812241694",
)

# Script generated for node stedi_customer_curated
stedi_customer_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690812241694,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi/customers_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="stedi_customer_curated_node3",
)

job.commit()
