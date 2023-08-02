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

# Script generated for node stedi-customer-landing
stedicustomerlanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi/customer_landing/"],
        "recurse": True,
    },
    transformation_ctx="stedicustomerlanding_node1",
)

# Script generated for node Filter
Filter_node1690778500955 = Filter.apply(
    frame=stedicustomerlanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1690778500955",
)

# Script generated for node stedi-customer-trusted
stedicustomertrusted_node1690778747245 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1690778500955,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi/customer_trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="stedicustomertrusted_node1690778747245",
)

job.commit()
