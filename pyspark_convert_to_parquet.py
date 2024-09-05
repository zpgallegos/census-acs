"""
ACS IPUMS 1-year: 
- apply a predefined schema to the csv data 
- partition and convert to efficent storage format

table created by this script is ready to be crawled by Glue
"""

import sys
import boto3
import logging

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, functions as F, Window as W

BUCKET = "powerlytics-ipums"
TBL = "ipums_2022_1year"
IN_PREF = "csv"
OUT_PREF = "parquet"

IN_PATH, OUT_PATH = (f"s3://{BUCKET}/{pref}/{TBL}" for pref in (IN_PREF, OUT_PREF))

SCHEMA_FILE = f"schemas/{TBL}-schema.txt"

logging.basicConfig(level=logging.INFO)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")


def load_schema() -> str:
    """
    get the schema for the IPUMS 1-year data set

    :return: str, schema as a string to be passed to spark.read.csv
    """
    obj = s3.get_object(Bucket=BUCKET, Key=SCHEMA_FILE)
    return obj["Body"].read().decode("utf-8")


schema = load_schema()

df = spark.read.format("csv").schema(schema).load(IN_PATH)

# partition by state, convert to parquet
df.write.format("parquet").partitionBy("ST").mode("append").save(OUT_PATH)

job.commit()
