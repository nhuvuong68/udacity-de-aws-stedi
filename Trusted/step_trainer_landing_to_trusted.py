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

# Script generated for node Step trainer landing
Steptrainerlanding_node1702113160351 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="Steptrainerlanding_node1702113160351",
)

# Script generated for node Customer Curated
CustomerCurated_node1702113201339 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1702113201339",
)

# Script generated for node Join with Customer Curated
SqlQuery108 = """
select * from step_trainer_landing
join customer_curated on step_trainer_landing.serialnumber = customer_curated.serialnumber
"""
JoinwithCustomerCurated_node1702113251707 = sparkSqlQuery(
    glueContext,
    query=SqlQuery108,
    mapping={
        "step_trainer_landing": Steptrainerlanding_node1702113160351,
        "customer_curated": CustomerCurated_node1702113201339,
    },
    transformation_ctx="JoinwithCustomerCurated_node1702113251707",
)

# Script generated for node Step trainer trusted
Steptrainertrusted_node1702113314951 = glueContext.getSink(
    path="s3://de-aws-nv-stedi-lake-house/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="Steptrainertrusted_node1702113314951",
)
Steptrainertrusted_node1702113314951.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
Steptrainertrusted_node1702113314951.setFormat("json")
Steptrainertrusted_node1702113314951.writeFrame(
    JoinwithCustomerCurated_node1702113251707
)
job.commit()
