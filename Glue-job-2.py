#Glue Job

#Code for preprocessing data to generate final output data

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

predicate_pushdown = "region in ('ca', 'gb', 'us')"

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732047894283 = glueContext.create_dynamic_frame.from_catalog(database="de_youtube_raw", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1732047894283", push_down_predicate = predicate_pushdown)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1732047873277 = glueContext.create_dynamic_frame.from_catalog(database="de_youtube_raw", table_name="cleaned_statistics_refrence_data", transformation_ctx="AWSGlueDataCatalog_node1732047873277")

# Script generated for node Join
Join_node1732047923046 = Join.apply(frame1=AWSGlueDataCatalog_node1732047894283, frame2=AWSGlueDataCatalog_node1732047873277, keys1=["category_id"], keys2=["id"], transformation_ctx="Join_node1732047923046")

# Script generated for node Amazon S3
AmazonS3_node1732048115782 = glueContext.getSink(path="s3://youtube-analytics-np", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1732048115782")
AmazonS3_node1732048115782.setCatalogInfo(catalogDatabase="db_youtube_analytics",catalogTableName="final-analytics")
AmazonS3_node1732048115782.setFormat("glueparquet", compression="snappy")
AmazonS3_node1732048115782.writeFrame(Join_node1732047923046)
job.commit()