import os
import sys
import numpy as np

os.environ['PYLIB']=os.environ['SPARK_HOME']+'/python/lib'
sys.path.insert(0,os.environ['PYLIB']+'/py4j-0.10.9-src.zip')
sys.path.insert(1,os.environ['PYLIB']+'/pyspark.zip')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDDTransformations') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))


ardd = sc.parallelize(range(1, 5))
pair_rdd = ardd.map(lambda x: (x, x))
print('reduce, ', ardd.reduce(lambda x, y: x + y))
print('collect, ', ardd.collect())
print('keys, ', pair_rdd.keys().collect())
print('values, ', pair_rdd.values().collect())
print('aggregate, ', ardd.aggregate((0, 0), lambda acc, vlu: (acc[0] + 1, acc[1] + vlu),
                                    lambda x, y: (x[0] + y[0], x[1] + y[1])))
print('first, ' , ardd.first())
print('take, ', ardd.take(2))
print('top, ', ardd.sortBy(lambda x: x, False).top(2))
# def sample(withReplacement: Boolean, fraction: Double, seed: Int): RDD[T]
print('sample, ', ardd.sample(True, 2, 5).collect())

# ######################################################################
# actions on pair rdds
# ######################################################################

print('collect as map, ', pair_rdd.collectAsMap())

print('count by key ', ardd.flatMap(lambda x: range(x, 5)).map(lambda x: (x, x)).countByKey())
print('count by value ', ardd.flatMap(lambda x: range(x, 5)).map(lambda x: (x, x)).countByValue())

# for rdds of doubles, some numerical computations are automaticaly enabled
arddDbl = sc.parallelize([float(x) for x in range(1, 5)])

print("Min: " , ardd.min() , ", Max: " , ardd.max() , " Sum: " , ardd.sum())
print("Mean: " , arddDbl.mean() , ", StDev: " , arddDbl.stdev() , ", Variance: " ,
      arddDbl.variance())
print("All together in stats: " , arddDbl.stats())

randlist = np.random.randint(0, 100, 100)
randRDD = sc.parallelize(randlist)
histogram = randRDD.histogram(10)
print('buckets and frequency of the histogram generated: ', [x for x in zip(histogram[0], histogram[1])])

print("\nSaving pairRDD to text file - directory will be created with one file for each partition")
txtfile_save_location = "hdfs://localhost:8020/user/samar/prdd_text_py"
pair_rdd.saveAsTextFile(txtfile_save_location)

print("Loading from the saved text file")
sc.textFile(txtfile_save_location).collect()

hdp_rdd = sc.newAPIHadoopFile(txtfile_save_location,
                              'org.apache.hadoop.mapreduce.lib.input.TextInputFormat', 'org.apache.hadoop.io.LongWritable',
                              'org.apache.hadoop.io.Text')

hdp_rdd.map(lambda x: (x[0], x[1])).collect()

pairDF = pair_rdd.toDF(["k", "v"])
print(pairDF.collect())
parquet_file_location = "hdfs://localhost:8020/user/cloudera/prdd_parquet_py"
pairDF.write.parquet(parquet_file_location)
print('reading from stored parquet file ',
spark.read.parquet(parquet_file_location).collect())


