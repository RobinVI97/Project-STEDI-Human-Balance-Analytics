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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1720425891985 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1720425891985")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1720426138996 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1720426138996")

# Script generated for node SQL Query
SqlQuery1229 = '''
select  *
from    step_trainer_trusted a
join    accelerometer_trusted b 
on  a.sensorreadingtime = b.timestamp 

'''
SQLQuery_node1720426244363 = sparkSqlQuery(glueContext, query = SqlQuery1229, mapping = {"step_trainer_trusted":StepTrainerTrusted_node1720425891985, "accelerometer_trusted":AccelerometerTrusted_node1720426138996}, transformation_ctx = "SQLQuery_node1720426244363")

# Script generated for node Amazon S3
AmazonS3_node1720426480846 = glueContext.getSink(path="s3://rvi-bucket-project/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1720426480846")
AmazonS3_node1720426480846.setCatalogInfo(catalogDatabase="stedi_hba",catalogTableName="machine_learning_curated")
AmazonS3_node1720426480846.setFormat("json")
AmazonS3_node1720426480846.writeFrame(SQLQuery_node1720426244363)
job.commit()
