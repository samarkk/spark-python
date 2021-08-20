import os
import sys
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 pyspark-shell'

spark = SparkSession.builder.appName('SparkCassandraApp') \
    .config('spark.cassandra.connection.host', argv[1]) \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.sql.extensions','com.datastax.spark.connector.CassandraSparkExtensions') \
    .getOrCreate()

print(spark.version)

sc = spark.sparkContext
sc.setLogLevel('ERROR')

spark.read.format("org.apache.spark.sql.cassandra") \
    .option('table', 'ttbl').option('keyspace', 'testks') \
    .load().show()
