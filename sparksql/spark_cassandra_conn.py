# pyspark -packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions  --conf spark.cassandra.connection.host=localhost
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

# spark cassandra connector is needed
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages com.datastax.spark:spark-cassandra-connector_2.12:3.0.0 pyspark-shell'

# with CassandraSparkExtensions spark catalyst catalog can be
# used to push calalog operations to Cassandra itself
spark = SparkSession.builder.appName('SparkCassandraApp') \
    .config('spark.cassandra.connection.host', argv[1]) \
    .config('spark.cassandra.connection.port', '9042') \
    .config('spark.sql.extensions', 'com.datastax.spark.connector.CassandraSparkExtensions') \
    .getOrCreate()

print(spark.version)

sc = spark.sparkContext
sc.setLogLevel('ERROR')

# While setting up a Catalog can be done before starting your application
# it can also be done at runtime by setting spark.sql.catalog.anyname to
# com.datastax.spark.connector.datasource.CassandraCatalog In your SparkSession.

spark.conf.set('spark.sql.catalog.casscatalog', 'com.datastax.spark.connector.datasource.CassandraCatalog')

# use catalog to show namespaces and tables
spark.sql('SHOW NAMESPACES FROM casscatalog').show()
spark.sql('SHOW TABLES FROM casscatalog.finks').show()

# we can thus read cassandra table as
fotable = spark.read.table('casscatalog.finks.fotable')
fotable.show()
# Number of partitions created is give by
# spark.cassandra.input.split.sizeInMB which has a default value of 512 MB
# and we have a minimum of 1 + 2 * SparkContext.defaultParallelism

print('using sql to load cassandra catalog table')
fotable_sql = spark.sql('select * from casscatalog.finks.fotable')
fotable_sql.show()

# so for one day's data with a table size of 40MB
# with local[4] we will have 1 + 2 * 4  + 1
# with yarn with spark.default.parallelism at 2 we will have 1 + 2 * 2 + 1 - 6

print('Number of partitioins of the fotable loaded from cassandra: {}'.format(fotable.rdd.getNumPartitions()))

# Source catalog connection cabability is available from spark 3 onwards
# we can load cassandra data as a data frame
fodf = spark.read.format("org.apache.spark.sql.cassandra") \
    .option('table', 'fotable').option('keyspace', 'finks') \
    .load()

# carry out some transformations / analysis on the data frame

fodf_summarized = fodf.groupBy('symbol', 'expiry', 'instrument', 'option_typ') \
    .agg(count('*').alias('oientries'), \
         sum(col('oi')).alias('totoi'), \
         sum(col('trdval')).alias('totval'))

fodf_summarized.show()

# using sql to carry out the same operation
spark.sql('''
    select symbol, expiry, instrument, option_typ,
    count(*) as oientries,
    sum(oi) as totoi,
    sum(trdval) as totval
    from casscatalog.finks.fotable
    group by symbol, expiry, instrument, option_typ
''').show()

# save the summarized table to cassandra
# to append add on .mode('append')
fodf_summarized. \
    write. \
    parititionBy(['symbol']) \
    .saveAsTable('casscatalog.finks.fotsum')

# create the target table using sql
# specify partition and clustering columns
spark.sql("""
create table casscatalog.finks.fotsumsql(
    symbol string, 
    expiry string,
    instrument string,
    option_typ string,
    oientries int,
    totoi int,
    totval decimal)
    USING cassandra PARTITIONED BY
    (symbol)
    TBLPROPERTIES(
    clustering_key='expiry,instrument,option_typ'
    )
""")

# use mode append to write the dataframe to the partitioned and clustered
# cassandra table
fodf_summarized \
    .write \
    .mode('append') \
    .saveAsTable('casscatalog.finks.fotsumsql')

# check predicate pushdowns
# no filters
fodf.filter("symbol = 'INFY'").explain()
# partition key used for filter
fodf.filter("symbol = 'INFY' and expiry = '2018-03-28'").explain()
# partition key and clustering column used as per sequence for table creation used
fodf.filter("symbol = 'INFY' and expiry = '2018-03-28' and trdate = '2018-01-01'")
# table is clustered by trdtate, instrument, - therefore only partition key used
fodf.filter("symbol = 'INFY' and expiry = '2018-03-28' and instrument = 'FUTSTK'").explain()
# since order maintained all columns in filter clause used
fodf.filter("""
symbol = 'INFY' and expiry = '2018-03-28' and trdate = '2018-01-01' and instrument = 'FUTSTK'
""").explain()

spark.sql("""
select * from casscatalog.finks.fotable 
where symbol = 'INFY' and expiry = '2018-03-28' 
and trdate = '2018-01-01' and instrument = 'FUTSTK'
""").explain()
