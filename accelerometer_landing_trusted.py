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

# Script generated for node Accelerometer landing
Accelerometerlanding_node1711764152554 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometerlanding_node1711764152554")

# Script generated for node Customer Trusted
CustomerTrusted_node1711763789637 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1711763789637")

# Script generated for node SQL Query
SqlQuery1718 = '''
select a.user,a.timestamp,a.x,a.y,a.z from a inner join b 
on 
a.user=b.email
'''
SQLQuery_node1711766679262 = sparkSqlQuery(glueContext, query = SqlQuery1718, mapping = {"b":CustomerTrusted_node1711763789637, "a":Accelerometerlanding_node1711764152554}, transformation_ctx = "SQLQuery_node1711766679262")

# Script generated for node Amazon S3
AmazonS3_node1711766999553 = glueContext.getSink(path="s3://vjdeudanano/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1711766999553")
AmazonS3_node1711766999553.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1711766999553.setFormat("json")
AmazonS3_node1711766999553.writeFrame(SQLQuery_node1711766679262)
job.commit()