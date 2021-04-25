import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType


def run_transform(bucket_name):	
	# Create a spark session
	spark = SparkSession.builder.appName('emr etl job').getOrCreate()
	# Setup the logger to write to spark logs
	# noinspection PyProtectedMember
	logger = spark._jvm.org.apache.log4j.Logger.getLogger('TRANSFORM')
	logger.info('Spark session created')
	logger.info('Trying to read data now.')
	logger.info(f'Bucket name is {bucket_name}')
	# Schema for the raw data json files
	raw_data_schema = StructType()
	raw_data_schema.add('uuid', 'string')
	raw_data_schema.add('device_ts', 'string')
	raw_data_schema.add('device_id', 'integer')
	raw_data_schema.add('device_temp', 'integer')
	raw_data_schema.add('track_id', 'integer')
	raw_data_schema.add('activity_type', 'string')
	# Schema for the reference data file
	reference_data_schema = StructType()
	reference_data_schema.add('track_id', 'string')
	reference_data_schema.add('track_name', 'string')
	reference_data_schema.add('artist_name', 'string')
	# Read the raw data into a data frame
	raw_schema = spark.read.schema(raw_data_schema)
	raw_data = raw_schema.json(f's3://{bucket_name}/data/raw/*/*/*/*/')
	# Read the reference data into a data frame
	reference_schema = spark.read.schema(reference_data_schema)
	reference_data = reference_schema.json(
		f's3://{bucket_name}/data/reference/'
	)
	raw_data.printSchema()
	reference_data.printSchema()
	logger.info(f'raw_data (count) = {raw_data.count()}')
	logger.info(f'reference_data (count) = {reference_data.count()}')
	# Join the two datasets on track_id
	joined_data = raw_data.join(reference_data, 'track_id', 'inner')
	joined_data.printSchema()
	# Bucket the data by activity type and write the
	# results to S3 in overwrite mode
	writer = joined_data.write
	writer.format('parquet')
	writer.mode('overwrite')
	writer.option('path', f's3://{bucket_name}/data/emr-processed/')
	writer.save()
	# Stop Spark
	spark.stop()


def main():
	# Accept bucket name from the arguments passed.
	# TODO: Error handling when there are no arguments passed.
	bucket_name = sys.argv[1]
	# Run the transform method
	run_transform(bucket_name)


if __name__ == '__main__':
	main()
