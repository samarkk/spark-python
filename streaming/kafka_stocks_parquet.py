# to run this in a terminal at location /home/cloudera
#  testkfp.sh /home/samar/data/201819/cm 2018 JAN 200 1
# go to hive and empty stocksagg and stocksaggckpt
# start writing to kafka topic and start this in pyspark console

from pyspark.sql import  SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('KafkaStocksParquet') \
    .config('spark.sql.shuffle.partitions', 2) \
    .enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

stock_quotes = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nsecmd") \
    .option("startingffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select("KEY", "VALUE", "TIMESTAMP")

split_col = split('VALUE', ',')

stocks_df = stock_quotes.withColumn('symbol', split_col.getItem(0)) \
    .withColumn('clspr', split_col.getItem(5)) \
    .withColumn('qty', split_col.getItem(8)) \
    .withColumn('vlu', split_col.getItem(9)) \
    .selectExpr('symbol', 'qty', 'vlu', 'clspr', 'timestamp as tstamp')

stocks_aggregated_with_window = stocks_df.withWatermark('tstamp', '30 seconds') \
    .groupBy('symbol',
             window("tstamp", "10 seconds", "5 seconds").alias('wdw')) \
    .agg(sum('qty').cast('long').alias('totqty'), avg('qty').alias('avgqty'),
         sum('vlu').alias('sumval'), avg('vlu').alias('avgval'),
         min('clspr').cast('double').alias('mincls'), max('clspr').cast('double').alias('maxcls')) \
    .select("symbol", "wdw", "avgqty", "avgval", "totqty", "sumval", "mincls", "maxcls").coalesce(1)

stocks_aggregated_with_window.printSchema()

hdfs_save_path_location = "hdfs://localhost:8020/user/samar/stocksagg"
checkpoint_location = "hdfs://localhost:8020/user/samar/stocksaggckpt"

stocks_parquet_query = stocks_aggregated_with_window.writeStream \
    .format("parquet") \
    .option("path", hdfs_save_path_location) \
    .option("checkpointLocation", checkpoint_location)

pq = stocks_parquet_query.start()

# stocksAggQuery.stop()
# streaming query monitoring, querying options
# query.id()          # get the unique identifier of the running query that persists across restarts from checkpoint data

# query.runId()       # get the unique id of this run of the query, which will be generated at every start/restart

# query.name()        # get the name of the auto-generated or user-specified name

# query.explain()   # print detailed explanations of the query

# query.stop()      # stop the query

# query.awaitTermination()   # block until query is terminated, with stop() or with error

# query.exception()       # the exception if the query has been terminated with error

# query.recentProgress()  # an array of the most recent progress updates for this query

# query.lastProgress()    # the most recent progress update of this streaming query
