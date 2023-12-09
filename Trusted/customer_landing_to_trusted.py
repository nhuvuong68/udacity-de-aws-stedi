import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1701966773951 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://de-aws-nv-stedi-lake-house/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1701966773951",
)

# Script generated for node Filter
Filter_node1701966907879 = Filter.apply(
    frame=CustomerLanding_node1701966773951,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filter_node1701966907879",
)

# Script generated for node Amazon S3
AmazonS3_node1701967004147 = glueContext.getSink(
    path="s3://de-aws-nv-stedi-lake-house/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1701967004147",
)
AmazonS3_node1701967004147.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
AmazonS3_node1701967004147.setFormat("json")
AmazonS3_node1701967004147.writeFrame(Filter_node1701966907879)
job.commit()
