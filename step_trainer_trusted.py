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

# Script generated for node step_trainer_landing
step_trainer_landing_node1711744796184 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1711744796184")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1711744777949 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1711744777949")

# Script generated for node SQL Query
SqlQuery1792 = '''
select distinct st.sensorreadingtime, st.serialnumber,st.distancefromobject from st,a
where st.sensorreadingtime=a.timestamp
'''
SQLQuery_node1711767577105 = sparkSqlQuery(glueContext, query = SqlQuery1792, mapping = {"st":step_trainer_landing_node1711744796184, "a":Accelerometer_trusted_node1711744777949}, transformation_ctx = "SQLQuery_node1711767577105")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1711744805499 = glueContext.getSink(path="s3://vjdeudanano/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1711744805499")
step_trainer_trusted_node1711744805499.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1711744805499.setFormat("json")
step_trainer_trusted_node1711744805499.writeFrame(SQLQuery_node1711767577105)
job.commit()