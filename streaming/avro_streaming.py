import os
import sys

from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column
from pyspark.sql.functions import col, struct
# with spark 3.0 we get the from_avro and to_avro functions directly
# from pyspark.sql.avro.functions import  from_avro,to_avro

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages org.apache.spark:spark-avro_2.12:3.0.0 pyspark-shell'

spark = SparkSession.builder.appName('Avro Streaming') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

# prior to spark 3 we can use these workaround functions to have from_avro and to_avro
def from_avro(col, jsonFormatSchema):
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").from_avro
    return Column(f(_to_java_column(col), jsonFormatSchema))


def to_avro(col):
    sc = SparkContext._active_spark_context
    avro = sc._jvm.org.apache.spark.sql.avro
    f = getattr(getattr(avro, "package$"), "MODULE$").to_avro
    return Column(f(_to_java_column(col)))

avro_type_struct = """
{
  "type": "record",
  "name": "struct",
  "fields": [
    {"name": "col1", "type": "long"},
    {"name": "col2", "type": "string"}
  ]
}"""


df = spark.range(10).select(struct(
    col("id"),
    col("id").cast("string").alias("id2")
).alias("struct"))

avro_struct_df = df.select(to_avro(col("struct")).alias("avro"))
avro_struct_df.show(3)