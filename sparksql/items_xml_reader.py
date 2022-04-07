import os
import sys
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

spark = SparkSession.builder.appName('SparkXML').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')

items_xml_location = 'D:/ufdata/items.xml'

items_df = spark.read.format("xml") \
    .options(rootTag="items", rowTag="item", valueTag=True) \
    .load(items_xml_location)

items_df.show(10, False)

items_df.withColumn('venue_batters',
                    explode_outer(col('venue.batters.batter'))) \
    .show(10, False)
