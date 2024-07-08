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

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1720079419488 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1720079419488")

# Script generated for node Customer Curated
CustomerCurated_node1720076005589 = glueContext.create_dynamic_frame.from_catalog(database="stedi_hba", table_name="customer_curated", transformation_ctx="CustomerCurated_node1720076005589")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1720079718927 = ApplyMapping.apply(frame=CustomerCurated_node1720076005589, mappings=[("customername", "string", "right_customername", "string"), ("email", "string", "right_email", "string"), ("phone", "string", "right_phone", "string"), ("birthday", "string", "right_birthday", "string"), ("serialnumber", "string", "right_serialnumber", "string"), ("registrationdate", "long", "right_registrationdate", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long")], transformation_ctx="RenamedkeysforJoin_node1720079718927")

# Script generated for node SQL Query
SqlQuery1411 = '''
select      a.*
from        step_trainer_landing a 
where       serialnumber in (select right_serialnumber from customer_curated)
'''
SQLQuery_node1720417807442 = sparkSqlQuery(glueContext, query = SqlQuery1411, mapping = {"step_trainer_landing":StepTrainerLanding_node1720079419488, "customer_curated":RenamedkeysforJoin_node1720079718927}, transformation_ctx = "SQLQuery_node1720417807442")

# Script generated for node Step Trainer to Trusted
StepTrainertoTrusted_node1720076682911 = glueContext.getSink(path="s3://rvi-bucket-project/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainertoTrusted_node1720076682911")
StepTrainertoTrusted_node1720076682911.setCatalogInfo(catalogDatabase="stedi_hba",catalogTableName="step_trainer_trusted")
StepTrainertoTrusted_node1720076682911.setFormat("json")
StepTrainertoTrusted_node1720076682911.writeFrame(SQLQuery_node1720417807442)
job.commit()
