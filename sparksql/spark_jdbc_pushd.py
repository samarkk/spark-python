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
prop = {'user': 'root', 'password': 'abcd', 'driver': 'com.mysql.jdbc.Driver'}
url = 'jdbc:mysql://asuspc.localdomain:3306/testdb'

mockdf = spark.read.format('jdbc') \
    .option('url', url) \
    .option('dbtable', 'mocktbl') \
    .option('user', 'root').option('password', 'abcd') \
    .option('driver', 'com.mysql.cj.jdbc.Driver') \
    .load()

mockdf.show()
