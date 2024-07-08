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

# Script generated for node Customer Trusted
CustomerTrusted_node1719912780670 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1719912780670")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1719912807219 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="accelerometer_trusted", transformation_ctx="Accelerometer_trusted_node1719912807219")

# Script generated for node Join
Join_node1719912923719 = Join.apply(frame1=CustomerTrusted_node1719912780670, frame2=Accelerometer_trusted_node1719912807219, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1719912923719")

# Script generated for node select customer columns
SqlQuery1138 = '''
select  distinct customername
,       email
,       phone 
,       birthday
,       serialnumber
,       registrationdate
,       lastupdatedate
,       sharewithresearchasofdate
,       sharewithpublicasofdate
,       sharewithfriendsasofdate
from    myDataSource
'''
selectcustomercolumns_node1719915582956 = sparkSqlQuery(glueContext, query = SqlQuery1138, mapping = {"myDataSource":Join_node1719912923719}, transformation_ctx = "selectcustomercolumns_node1719915582956")

# Script generated for node Amazon S3
AmazonS3_node1719912960974 = glueContext.getSink(path="s3://rvi-bucket-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1719912960974")
AmazonS3_node1719912960974.setCatalogInfo(catalogDatabase="stedi_hba",catalogTableName="customer_curated")
AmazonS3_node1719912960974.setFormat("json")
AmazonS3_node1719912960974.writeFrame(selectcustomercolumns_node1719915582956)
job.commit()
