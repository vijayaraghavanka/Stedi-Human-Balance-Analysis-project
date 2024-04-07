import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1710822890555 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://vjdeudanano/customer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1710822890555")

# Script generated for node SQL Query
SqlQuery778 = '''
select * from myDataSource
where sharewithresearchasofdate !=0
'''
SQLQuery_node1711577545865 = sparkSqlQuery(glueContext, query = SqlQuery778, mapping = {"myDataSource":AmazonS3_node1710822890555}, transformation_ctx = "SQLQuery_node1711577545865")

# Script generated for node Customer Trusted
CustomerTrusted_node1710822924150 = glueContext.getSink(path="s3://vjdeudanano/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerTrusted_node1710822924150")
CustomerTrusted_node1710822924150.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
CustomerTrusted_node1710822924150.setFormat("json")
CustomerTrusted_node1710822924150.writeFrame(SQLQuery_node1711577545865)
job.commit()