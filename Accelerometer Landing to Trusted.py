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

# Script generated for node  Customer Trusted
CustomerTrusted_node1711681104680 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiLine": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://vjdeudanano/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1711681104680",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1711718405114 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="AWSGlueDataCatalog_node1711718405114",
)

# Script generated for node Join
Join_node1711681099780 = Join.apply(
    frame1=CustomerTrusted_node1711681104680,
    frame2=AWSGlueDataCatalog_node1711718405114,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1711681099780",
)

# Script generated for node Drop Fields
DropFields_node1711681608700 = DropFields.apply(
    frame=Join_node1711681099780,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1711681608700",
)

# Script generated for node Amazon S3
AmazonS3_node1711681779118 = glueContext.getSink(
    path="s3://vjdeudanano/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1711681779118",
)
AmazonS3_node1711681779118.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1711681779118.setFormat("json")
AmazonS3_node1711681779118.writeFrame(DropFields_node1711681608700)
job.commit()
