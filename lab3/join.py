import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## @type: DataSource
## @args: [database = "crawler_db", table_name = "raw", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "crawler_db", table_name = "raw", transformation_ctx = "datasource0")

## @type: DataSource
## @args: [database = "crawler_db", table_name = "reference", transformation_ctx = "src_reference"]
## @return: src_reference
## @inputs: []
src_reference = glueContext.create_dynamic_frame.from_catalog(database = "crawler_db", table_name = "reference", transformation_ctx = "src_reference")

## @type: Join
## @args: [keys1 = "track_id", keys2 = "track_id"]
## @return: joined_data
## @inputs: [frame1 = datasource0, frame2 = src_reference]
joined_data = Join.apply(frame1 = datasource0, frame2 = src_reference, keys1 = ['track_id'], keys2 = ['track_id'], transformation_ctx = "joined_data")

## @type: DropFields
## @args: [paths = ['partition_0', 'partition_1', 'partition_2', 'partition_3'], transformation_ctx = "drop_fields"]
## @return: joined_data_clean
## @inputs: [frame = joined_data]
joined_data_clean = DropFields.apply(frame = joined_data, paths = ['partition_0', 'partition_1', 'partition_2', 'partition_3'], transformation_ctx = "drop_fields")

## @type: DataSink
## @args: [connection_type = "s3", connection_options = {"path": "s3://cedchan-analyticstfc-datalake-demo/data/processed/"}, format = "parquet", transformation_ctx = "datasink4"]
## @return: datasink4
## @inputs: [frame = joined_data_clean]
datasink4 = glueContext.write_dynamic_frame.from_options(frame = joined_data_clean, connection_type = "s3", connection_options = {"path": "s3://cedchan-analyticstfc-datalake-demo/data/processed/"}, format = "parquet", transformation_ctx = "datasink4")

job.commit()
