from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

def run_kafka_streaming():
    spark = SparkSession.builder.appName('StructuredKafkaWordCount') \
        .config('spark.sql.shuffle.partitions', 2) \
        .enableHiveSupport().getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    lines = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "wci") \
        .option("startingOffsets", "earliest") \
        .load().selectExpr("cast(value as string) word", "timestamp")

    lines.toDF('word', 'timestamp')\
        .select(explode(split('word', ' ')).alias('word'), 'timestamp') \
        .groupBy(window('timestamp', '4 seconds', '2 seconds'), 'word').count() \
        .writeStream.format('console').outputMode('complete').option('truncate', False) \
        .start() \
        .awaitTermination()

if __name__ == "__main__":
    run_kafka_streaming()