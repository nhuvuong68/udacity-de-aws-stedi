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


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1702113877308 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrusted_node1702113877308",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702113726903 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1702113726903",
)

# Script generated for node Join
SqlQuery99 = """
SELECT * FROM step_trainer_trusted 
join accelerometer_trusted on step_trainer_trusted.sensorreadingtime = accelerometer_trusted.timestamp
"""
Join_node1702114143130 = sparkSqlQuery(
    glueContext,
    query=SqlQuery99,
    mapping={
        "step_trainer_trusted": StepTrainerTrusted_node1702113877308,
        "accelerometer_trusted": AccelerometerTrusted_node1702113726903,
    },
    transformation_ctx="Join_node1702114143130",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1702114209492 = glueContext.getSink(
    path="s3://de-aws-nv-stedi-lake-house/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1702114209492",
)
MachineLearningCurated_node1702114209492.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1702114209492.setFormat("json")
MachineLearningCurated_node1702114209492.writeFrame(Join_node1702114143130)
job.commit()
