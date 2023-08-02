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

# Script generated for node stedi_step_trainer_trusted
stedi_step_trainer_trusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="glue-database",
    table_name="stedi_step_trainer_trusted",
    transformation_ctx="stedi_step_trainer_trusted_node1",
)

# Script generated for node stedi_accelerometer_trusted
stedi_accelerometer_trusted_node1690975018390 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="glue-database",
        table_name="stedi_accelerometer_trusted",
        transformation_ctx="stedi_accelerometer_trusted_node1690975018390",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690975084988 = ApplyMapping.apply(
    frame=stedi_accelerometer_trusted_node1690975018390,
    mappings=[
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("z", "double", "right_z", "double"),
        ("timestamp", "long", "right_timestamp", "long"),
        ("birthday", "string", "right_birthday", "string"),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("customername", "string", "right_customername", "string"),
        ("user", "string", "right_user", "string"),
        ("y", "double", "right_y", "double"),
        ("x", "double", "right_x", "double"),
        ("email", "string", "right_email", "string"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690975084988",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1690975131315 = ApplyMapping.apply(
    frame=RenamedkeysforJoin_node1690975084988,
    mappings=[
        ("right_serialnumber", "string", "acc__right_serialnumber", "string"),
        ("right_z", "double", "acc__right_z", "double"),
        ("right_timestamp", "long", "acc__right_timestamp", "long"),
        ("right_birthday", "string", "acc__right_birthday", "string"),
        (
            "right_sharewithpublicasofdate",
            "long",
            "acc__right_sharewithpublicasofdate",
            "long",
        ),
        (
            "right_sharewithresearchasofdate",
            "long",
            "acc__right_sharewithresearchasofdate",
            "long",
        ),
        ("right_registrationdate", "long", "acc__right_registrationdate", "long"),
        ("right_customername", "string", "acc__right_customername", "string"),
        ("right_user", "string", "acc__right_user", "string"),
        ("right_y", "double", "acc__right_y", "double"),
        ("right_x", "double", "acc__right_x", "double"),
        ("right_email", "string", "acc__right_email", "string"),
        ("right_lastupdatedate", "long", "acc__right_lastupdatedate", "long"),
        ("right_phone", "string", "acc__right_phone", "string"),
        (
            "right_sharewithfriendsasofdate",
            "long",
            "acc__right_sharewithfriendsasofdate",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1690975131315",
)

# Script generated for node Join
Join_node1690975044046 = Join.apply(
    frame1=stedi_step_trainer_trusted_node1,
    frame2=RenamedkeysforJoin_node1690975131315,
    keys1=["right_timestamp"],
    keys2=["acc__right_timestamp"],
    transformation_ctx="Join_node1690975044046",
)

# Script generated for node Amazon S3
AmazonS3_node1690975262123 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1690975044046,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://project-stedi/machine_learning_curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AmazonS3_node1690975262123",
)

job.commit()
