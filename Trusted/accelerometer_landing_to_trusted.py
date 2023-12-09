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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1702106575844 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": True},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1702106575844",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1702106620166 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1702106620166",
)

# Script generated for node Join privacy customer
SqlQuery146 = """
select * from accelerometer_landing inner join customer_trusted on accelerometer_landing.user = customer_trusted.email
"""
Joinprivacycustomer_node1702108175107 = sparkSqlQuery(
    glueContext,
    query=SqlQuery146,
    mapping={
        "accelerometer_landing": AccelerometerLanding_node1702106575844,
        "customer_trusted": CustomerTrusted_node1702106620166,
    },
    transformation_ctx="Joinprivacycustomer_node1702108175107",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1702108285363 = glueContext.getSink(
    path="s3://de-aws-nv-stedi-lake-house/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1702108285363",
)
AccelerometerTrusted_node1702108285363.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1702108285363.setFormat("json")
AccelerometerTrusted_node1702108285363.writeFrame(Joinprivacycustomer_node1702108175107)
job.commit()
