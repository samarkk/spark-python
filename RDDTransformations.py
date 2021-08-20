import os
import numpy as np
from pyspark.sql.functions import *

os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDDTransformations') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

# map  - single element transformed to another
a_coll = ['a', 'b', 'c']
a_coll_rdd = sc.parallelize(a_coll)
a_coll_rdd.map(lambda x: (x, 1)).collect()

# filter
no_rdd = sc.parallelize(range(10))
print('Filtering to print only odd numbers \n',
      no_rdd.filter(lambda x: x % 2 != 0).collect())

#flatmap - each element map to a collection and then flatten
# one to three rdd
ott_rdd = sc.parallelize([1, 2, 3])
print('Flatmapping illustration \n')
ott_rdd.flatMap(lambda x: (x, x * 100, 42)).collect()

# distinct
print('Distinct\n')
rpt_list = np.random.randint(0, 5, 10)
print('Randomly generated list with repeat elements ', rpt_list)
rpt_rdd = sc.parallelize(rpt_list)
print(rpt_rdd.distinct().collect())

# A utillity function to print tuples in a list
def printTupleList(tlist):
    return [(x[0], list(x[1])) for x in tlist ]

print('RDD groupBy transformation ')
group_by_collect = no_rdd.groupBy(lambda x: x % 2).collect()

printTupleList(group_by_collect)

print('keyBy transformation')
new_no_rdd = sc.parallelize(range(10))
print(new_no_rdd.keyBy(lambda x: x % 2 == 0).collect())
print(new_no_rdd.keyBy(lambda x: x % 2 == 0).map(lambda x: ('even' if x[0] else 'odd', x[1])).collect())

# sorting rdds
rand_rdd = sc.parallelize(np.random.randint(0, 20, 10))
print("(rand_rdd.collect())")

print('Simple sort ascending')
rand_rdd.sortBy(lambda x: x).collect()

print('Simple sort descending')
rand_rdd.sortBy(lambda x: -x).collect()

# complex rdd sorting
comp_rdd = sc.parallelize([("arjun", "tendulkar", 5),
                           ("sachin", "tendulkar", 102), ("vachin", "tendulkar", 102),
                           ("rahul", "dravid", 74), ("vahul", "dravid", 74),
                           ("rahul", "shavid", 74), ("vahul", "shavid", 74),
                           ("jacques", "kallis", 92), ("ricky", "ponting", 84), ("jacques", "zaalim", 92),
                           ("sachin", "vendulkar", 102)])

comp_rdd.sortBy(lambda x: x[1], False).collect()

comp_df = comp_rdd.toDF(['fname', 'lname', 'centuries'])
comp_df.show()

comp_df.sort('lname', desc('fname'), desc('centuries')).show()

# ##################################################
# Two RDD Operations
# ##################################################

print('Union of two rdds:\n')
first = sc.parallelize([1, 2, 3])
second = sc.parallelize([3, 4])
print("union: ", first.union(second).collect())

print('Intersection of two rdds:\n')
print("union: ", first.intersection(second).collect())

# zipping rdds - should have same number of partitions and same number of elements in each partition
# first.zip(second).collect()
first.zip(sc.parallelize([3, 4, 5])).collect()

# ##################################################
#  Pair RDD - transformations and joins
# ##################################################
pow_lt3_rdd = sc.parallelize([(1, 1), (1, 1), (1, 1), (2, 2), (2, 4), (2, 8), (3, 3), (3, 9), (3, 27)])

printTupleList(pow_lt3_rdd.groupByKey().collect())

pow_lt3_rdd.reduceByKey(lambda x, y: x + y).collect()

xj = sc.parallelize([("a", 1), ("b", 2)])
yj = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
print(xj.join(yj).collect())

print("Left Outer Join")
xoj = sc.parallelize([("a", 1), ("b", 2), ("c", 7)])
yoj = sc.parallelize([("a", 3), ("a", 4), ("b", 5), ("d", 4)])
print(xoj.leftOuterJoin(yoj).collect())
print("Right Outer Join")
print(xoj.rightOuterJoin(yoj).collect())

print("Full Outer Join")
print(xoj.fullOuterJoin(yoj).collect())

# ####################################################
# rdd partitions - seeing, mapping,
# ####################################################

a_coll_rdd.getNumPartitions()

a_coll_rdd.glom().collect()

print("coalesce to reduce number of partitions")
xCoalesce = sc.parallelize(range(10), 4)
print(xCoalesce.getNumPartitions())
yCoalesce = xCoalesce.coalesce(2)
print(yCoalesce.getNumPartitions())

# # MapPartitions

def show_partitions(idx, itera):
    yield 'index: ' + str(idx) + ' , elements: ' + str(list(itera))

print('mapPartitionsWithIndex ')
a_coll_rdd.mapPartitionsWithIndex(show_partitions).collect()

def f(sidx, itr): yield sidx

a_coll_rdd.mapPartitionsWithIndex(f).collect()
# we require a function to iterate over the elements of a partition
def map_partitions_function(itera):
    yield ', '.join(list(itera))

a_coll_rdd.mapPartitions(map_partitions_function).collect()


# ####################################################
# rdd custom partitioner
# ####################################################
def part_function(k):
    return 0 if k < 'H' else 1

x_part = sc.parallelize([('J', "James"), ('F', "Fred"), ('A', "Anna"), ('J', "John")], 3)
x_part.partitionBy(2, part_function).mapPartitionsWithIndex(show_partitions).collect()

# ####################################################
# rdd complex transformations - aggregate, aggregateByKey, combineByKey
# ####################################################

# # RDD Aggregation
# ### A sequence operation which will run as a combiner of sort on the elements of the partition
# ### A combining operation which will reduce the combined tuples from the partitions to a single tuple

def seq_op(data, elem):
    return (data[0] + elem, data[1] + 1)

def comb_op(d1, d2):
    return (d1[0] + d2[0], d1[1] + d2[1])

ardd = sc.parallelize(range(1, 13), 6)
print('First take a look at the partitions of the rdd')
print(ardd.mapPartitionsWithIndex(show_partitions).collect())
print(ardd.aggregate((0,0), seq_op, comb_op))

print(ardd.aggregate((0, 0),
                     lambda data, elem: (data[0] + elem, data[1] + 1),
                     lambda d1, d2: (d1[0] + d2[0], d1[1] + d2[1])))

#  ### Tree aggregations operate exactly in the same way as aggreate  except for one critical difference -
#  there is an intermediate aggregation step  data from some partitions will be sent to executors to aggregate
#  ### so in the above case if there are six partitions  while aggregate will send
#  results of all the six partitions to the driver in tree aggregate, three will go to one executor,
#  three to another and the driver will receive the aggregations from 2 rather than 6
#  ### Where there are many number of partitions tree aggregate performs significantly better than vanilla aggregate

print(ardd.treeAggregate((0,0), seq_op, comb_op))

frd = sc.parallelize([(1, 1), (2, 4), (3, 9), (1, 1), (2, 8), (3, 27),
                      (1, 1), (2, 16), (3, 81), (4, 256)])

frd.combineByKey(lambda x: (x, 1),
                 lambda acc, vlu: (acc[0] + vlu, acc[1] + 1),
                 lambda v1, v2: (v1[0] + v2[0], v1[1] + v2[1])).collect()

# ### aggregateByKey also will function at the partition level
# we need a value to seed the aggregation and we have a combination of
# a sequence operation and a combo operation playing out

frd.aggregateByKey(0,
                   lambda x, y: x + y,
                   lambda a, b: a + b
                   ).collect()
