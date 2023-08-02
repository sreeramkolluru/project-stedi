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

# Script generated for node stedi-step-trainer
stedisteptrainer_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi/step_trainer_landing/"],
        "recurse": True,
    },
    transformation_ctx="stedisteptrainer_node1",
)

# Script generated for node stedi-customer-curated
stedicustomercurated_node1690953313575 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://project-stedi/customers_curated/"],
        "recurse": True,
    },
    transformation_ctx="stedicustomercurated_node1690953313575",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690953436400 = ApplyMapping.apply(
    frame=stedicustomercurated_node1690953313575,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("`.customerName`", "string", "`right_.customerName`", "string"),
        ("z", "double", "right_z", "double"),
        ("timeStamp", "long", "right_timeStamp", "long"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithPublicAsOfDate", "long", "right_shareWithPublicAsOfDate", "long"),
        ("`.email`", "string", "`right_.email`", "string"),
        (
            "shareWithResearchAsOfDate",
            "long",
            "right_shareWithResearchAsOfDate",
            "long",
        ),
        ("registrationDate", "long", "right_registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("`.phone`", "string", "`right_.phone`", "string"),
        ("user", "string", "right_user", "string"),
        (
            "`.shareWithPublicAsOfDate`",
            "long",
            "`right_.shareWithPublicAsOfDate`",
            "long",
        ),
        ("y", "double", "right_y", "double"),
        ("`.lastUpdateDate`", "long", "`right_.lastUpdateDate`", "long"),
        ("`.birthDay`", "string", "`right_.birthDay`", "string"),
        ("x", "double", "right_x", "double"),
        ("`.registrationDate`", "long", "`right_.registrationDate`", "long"),
        ("`.serialNumber`", "string", "`right_.serialNumber`", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "long", "right_lastUpdateDate", "long"),
        (
            "`.shareWithResearchAsOfDate`",
            "long",
            "`right_.shareWithResearchAsOfDate`",
            "long",
        ),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "right_shareWithFriendsAsOfDate", "long"),
        (
            "`.shareWithFriendsAsOfDate`",
            "long",
            "`right_.shareWithFriendsAsOfDate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690953436400",
)

# Script generated for node Join
Join_node1690953409635 = Join.apply(
    frame1=stedisteptrainer_node1,
    frame2=RenamedkeysforJoin_node1690953436400,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1690953409635",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node3 = glueContext.getSink(
    path="s3://project-stedi/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node3",
)
step_trainer_trusted_node3.setCatalogInfo(
    catalogDatabase="glue-database", catalogTableName="stedi_step_trainer_trusted"
)
step_trainer_trusted_node3.setFormat("json")
step_trainer_trusted_node3.writeFrame(Join_node1690953409635)
job.commit()
