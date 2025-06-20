from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

from config import configuration

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
    .config("spark.jars.packages", 
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
        "org.apache.hadoop:hadoop-aws:3.4.1,"
        "com.amazonaws:aws-java-sdk:1.12.770") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .getOrCreate()
    

    spark.sparkContext.setLogLevel('WARN')

    #Schema's
    vehicleSchema = StructType([
        StructField("id", StringType(),True),
        StructField("deviceID", StringType(),True),
        StructField("timestamp", TimestampType(),True),
        StructField("location", StringType(),True),
        StructField("speed", DoubleType(),True),
        StructField("direction", StringType(),True),
        StructField("make", StringType(),True),
        StructField("model", StringType(),True),
        StructField("year", IntegerType(),True),
        StructField("fuelType", StringType(),True)
    ])

    gpsSchema = StructType([
        StructField("id", StringType(),True),
        StructField("deviceID", StringType(),True),
        StructField("timestamp", TimestampType(),True),
        StructField("speed", DoubleType(),True),
        StructField("direction", StringType(),True),
        StructField("vehicle_type", StringType(),True)
    ]) 

    trafficSchema = StructType([
        StructField("id", StringType(),True),
        StructField("deviceID", StringType(),True),
        StructField("cameraID", StringType(),True),
        StructField("timestamp", TimestampType(),True),
        StructField("location", StringType(),True),
        StructField("snapshot", StringType(),True)
    ]) 

    weatherSchema = StructType([
        StructField("id", StringType(),True),
        StructField("deviceID", StringType(),True),
        StructField("timestamp", TimestampType(),True),
        StructField("location", StringType(),True),
        StructField("temperature", DoubleType(),True),
        StructField("weatherCondition", StringType(),True),
        StructField("precipitation", DoubleType(),True),
        StructField("windspeed", DoubleType(),True),
        StructField("humidity", IntegerType(),True),
        StructField("airQualityIndex", DoubleType(),True) 
    ])


    def read_kafka_topic(topic, schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', 'broker:29092')
                .option('subscribe', topic)
                .option('startingOffsets', 'earliest')
                .load()
                .selectExpr('CAST(VALUE AS STRING)')
                .select(from_json(col('value'),schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '2 minutes')
                )


    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    vehicleDF= read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF= read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF= read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF= read_kafka_topic('weather_data', weatherSchema).alias('weather')


    query1 = streamWriter(vehicleDF, 's3a://kafka-spark-streaming-data/checkpoints/vehicle_data',
                 's3a://kafka-spark-streaming-data/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://kafka-spark-streaming-data/checkpoints/gps_data',
                 's3a://kafka-spark-streaming-data/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://kafka-spark-streaming-data/checkpoints/traffic_data',
                 's3a://kafka-spark-streaming-data/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://kafka-spark-streaming-data/checkpoints/weather_data',
                 's3a://kafka-spark-streaming-data/data/weather_data')


    query4.awaitTermination()


if __name__ == "__main__":
    main()
