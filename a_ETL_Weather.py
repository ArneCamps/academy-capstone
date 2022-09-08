import os
from pyspark import SparkContext


os.environ['PYSPARK_SUBMIT_ARGS'] = "org.apache.hadoop:hadoop-aws:3.1.2"


spark = SparkContext.getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()

config = configparser.ConfigParser()

config.read(os.path.expanduser("~/.aws/credentials"))

access_key = config.get("****", "aws_access_key_id")
secret_key = config.get("****", "aws_secret_access_key")
session_key = config.get("****", "aws_session_token")


sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")
sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");


hadoop_conf.set("fs.s3.aws.credentials.provider", "org.apache.hadoop.fs.s3.TemporaryAWSCredentialsProvider")
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.session.token", session_key)

s3_path = "s3://dataminded-academy-capstone-resources/raw/open_aq/*"

sparkDF = spark.read.json(s3_path) 
sparkDF.show()