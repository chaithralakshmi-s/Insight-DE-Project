from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
import configparser
import os
import logging

def run_spark(app_name, executor_memory='9gb'):
    config = configparser.ConfigParser()
    config.read(os.path.expanduser('~/.aws/credentials'))
    access_id = config.get('default', 'aws_access_key_id')
    access_key = config.get('default', 'aws_secret_access_key')
    spark = SparkSession.builder.appName(app_name).config('spark.executor.memory', executor_memory).getOrCreate()
    sc=spark.sparkContext
    hadoop_conf=sc._jsc.hadoopConfiguration()
    hadoop_conf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
    hadoop_conf.set('fs.s3a.awsAccessKeyId', access_id)
    hadoop_conf.set('fs.s3a.awsSecretAccessKey', access_key)
    sqlContext = SQLContext(sc)
    sqlctx=sqlContext
    logging.info("Spark context created")
    spark.sparkContext.setLogLevel('WARN')

    return spark,sqlctx
    
