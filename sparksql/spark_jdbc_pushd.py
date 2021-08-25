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

warehouse_dir_location = "D:/tmp"
mysql_jar_location = "D:/ufdata/mysql-connector-java-8.0.26.jar"

spark = SparkSession.builder.appName('SparkJDBC') \
    .config('spark.warehouse.dir', warehouse_dir_location) \
    .config('spark.driver.extraClassPath', mysql_jar_location) \
    .config('spark.executor.extraClassPath', mysql_jar_location) \
    .enableHiveSupport().getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')

print('spark version {}, spark context version {}'.format(spark.version, sc.version))

prop = {'user': 'root', 'password': 'puneetRai10198)', 'driver': 'com.mysql.jdbc.Driver'}
url = 'jdbc:mysql://localhost:3306/testdb'
mockdf = spark.read.format('jdbc').option('url', url).option('dbtable', 'mocktbl') \
    .option('user', 'root').option('password', 'puneetRai10198)') \
    .option('driver', 'com.mysql.jdbc.Driver').load()
mockdf.show()


mockdf.printSchema()

mockdf.rdd.getNumPartitions()

mockdf.groupBy('lname').count().show()

mockdf_part = spark.read.format('jdbc').option('url', url).option('dbtable', 'mocktbl') \
    .option('user', 'root').option('password', 'puneetRai10198)') \
    .option('driver', 'com.mysql.jdbc.Driver') \
    .option('partitionColumn', 'id').option('lowerBound', 0).option('upperBound', 1000) \
    .option('numPartitions', 4).load()
mockdf_part.show()

mockdf_part.rdd.getNumPartitions()

filter_query = "(select fname, lname from mocktbl where id between 10 and 20) fq"
push_down_df = spark.read.jdbc(url=url, table=filter_query, properties=prop)
push_down_df.show()

push_down_df.explain()

df_pushdown = spark.read.jdbc(table="mocktbl", url=url, properties=prop).where('id between 10 and 20')

df_pushdown.explain()

df_pushdown.show()

mockdf.write.jdbc(url=url, mode='append', table='mocktblcp', properties=prop)

spark.read.format('jdbc').option('url', url).option('dbtable', 'mocktblcp') \
    .option('user', 'root').option('password', 'puneetRai10198)') \
    .option('driver', 'com.mysql.jdbc.Driver').load().count()
