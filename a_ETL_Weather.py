import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from b_retrieveSnowFlakecredentials import retriefSnowflakeSecret


conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.1.2,net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3')
conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

 
spark = SparkSession.builder.config(conf=conf).getOrCreate()

def readandtransformweatherdata(path):
    return (spark.read.json(path)
                .select('city',
                        'coordinates.latitude',
                        'coordinates.longitude',
                        'country',
                        F.to_timestamp('date.local').alias('localtime'),
                        F.to_timestamp('date.utc').alias('utctime'),
                        'entity',
                        'isAnalysis',
                        'location',
                        'locationId',
                        'parameter',
                        'sensorType',
                        'unit',
                        'value'
                        )
                        )


