import os
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

os.environ['PYLIB']=os.environ['SPARK_HOME']+'/python/lib'
sys.path.insert(0,os.environ['PYLIB']+'/py4j-0.10.9-src.zip')
sys.path.insert(1,os.environ['PYLIB']+'/pyspark.zip')


spark = SparkSession.builder.appName('StructuredWordCount') \
    .config('spark.sql.shuffle.partitions', 2) \
    .enableHiveSupport().getOrCreate()

spark.conf.set('spark.sql.shuffle.partitions', 2)

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load() \
    .withColumn("timestamp", current_timestamp())

words = lines.toDF('word', 'timestamp').select(
    explode(split('word', ' ')).alias('word'), 'timestamp'
)

# simple word count query
word_counts = words.groupBy('word').\
    count()\

word_counts_query =  word_counts\
    .writeStream\
    .outputMode('complete')\
    .format('console')

word_counts_query.start().awaitTermination()

# word counts with windows
wiindowed_word_counts = words. \
    groupBy(
    window("timestamp", "10 seconds", "5 seconds"),
    "word").count()

window_query = wiindowed_word_counts \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .option("truncate", False)

window_query.start().awaitTermination()

# To get status of queries
spark.streams.active
spark.streams.active[n].status
spark.streams.active[n].lastProgress

# query with watermark
watermarked_window_count = words \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(
    window("timestamp", "2 minutes", "1 minutes"),
    "word").count().toDF("wdw", "word", "counts")

watermarked_window_count.printSchema()

hdfs_save_path_location = "hdfs://localhost:8020/user/samar/waterwrdc"
checkpoint_location = "file:///home/samar/watercheck"

watermarked_window_query = watermarked_window_count.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("truncate", False) \
    .option("path", hdfs_save_path_location) \
    .option("checkpointLocation", checkpoint_location)

watermarked_window_query.start().awaitTermination()

# stream batch table join to enrich stream with metadata information
# create the batch table
spark.sql('''
create table if not exists wimptbl(
word string, freq int)
          ''')

spark.sql("""
insert into wimptbl values
('java', 90), ('python', 60), ('spark', 40), ('airflow', 10),
('kafka', 35), ('scala', 20)
""")

batch_tbl = spark.read.table("wimptbl")

words.join(batch_tbl, ["word"], "left_outer") \
    .na.fill(0, ["freq"]) \
    .groupBy("word", "freq") \
    .count() \
    .withColumn("wtdfreq", col("freq") * col("count")) \
    .writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()
