# to run this in a terminal at location /home/cloudera
#  testkfp.sh /home/samar/data/201819/cm 2018 JAN 200 1
# we can have a console consumer checking nsecmd alongside
# and run this in pyspark console

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('KafkaStocksAggregation') \
    .config('spark.sql.shuffle.partitions', 2) \
    .enableHiveSupport().getOrCreate()

spark.sparkContext.setLogLevel('ERROR')

stock_quotes = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "nsecmd") \
    .option("startingffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .select("KEY", "VALUE")


split_col = split('VALUE', ',')

stocks_df = stock_quotes.withColumn('symbol', split_col.getItem(0)) \
    .withColumn('qty', split_col.getItem(8)) \
    .withColumn('vlu', split_col.getItem(9)) \
    .select('symbol', 'qty', 'vlu')

stocks_aggregated_query = stocks_df.groupBy('symbol') \
    .agg(sum('qty').alias('totqty'), avg('qty').alias('avgqty'),
         sum('vlu').alias('totvlu'), avg('vlu').alias('avgvlu')) \
    .writeStream.format('console') \
    .outputMode('update')

stocks_aggregated_query.start().awaitTermination()


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
