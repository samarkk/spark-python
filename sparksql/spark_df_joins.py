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

spark = SparkSession.builder.appName('Spark Dataframe Joins') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

# we have two Dataframes
# one for test matches and one for odi matches
# each has information for matches played, innings batted, runs scored and centuries made for
# Scahine Tendulkar and Virat Kohli for some years
# Combination of id and year makes each row unique

testDF = sc.parallelize([(1, "sachin", 2000, 6, 10, 575, 2), (1, "sachin", 2001, 10, 18, 1003, 3),
                         (1, "sachin", 2002, 16, 26, 1392, 4), (1, "sachin", 2010, 14, 23, 1562, 7),
                         (2, "virat", 2011, 5, 9, 202, 0), (2, "virat", 2012, 9, 16, 689, 3),
                         (2, "virat", 2016, 12, 18, 1215, 4), (2, "virat", 2017, 10, 16, 1059, 5)
                         ]).toDF(["id", "fname", "year", "matches", "innings", "runs", "centuries"])

odiDF = sc.parallelize([(1, 2000, "sachin", 34, 34, 1328, 3), (1, 2001, "sachin", 17, 16, 904, 4),
                        (1, 2010, "sachin", 2, 2, 1, 204), (1, 2011, "sachin", 11, 11, 513, 2),
                        (2, 2008, "virat", 5, 5, 159, 0), (2, 2009, "virat", 10, 8, 325, 1),
                        (2, 2016, "virat", 10, 10, 739, 3), (2, 2017, "virat", 26, 26, 1460, 6)
                        ]).toDF(["id", "year", "fname", "matches", "innings", "runs", "centuries"])

# cross join
# need to have this setting to cross joins to work as default
spark.conf.set("spark.sql.crossJoin.enabled", True)
testDF.join(odiDF).show()

# renaming columns when we already have a DF and we call toDF then we need
# to provide comma separated names for each of the columns
# in contrast, when we are parallelizing and callling to DF we have to provide the colnames as a list
testDF.toDF('fc', 'sc', 'tc', 'foc', 'fic', 'sic', 'sec').show()

# cross join and column renaming to create unique columns
from pyspark.sql.functions import *

# testDF.join(odiDF).select(testDF['id'], testDF['fname']).toDF("a", "b").show()
testDF.join(odiDF).toDF("id", "fname", "year", "tmatches", "tinnings",
                        "truns", "tcenturies", "sid", "syear", "sfname",
                        "omatches", "oinnings", "oruns", "ocenturies").show()

# implicit inner join
testDF.join(odiDF, "id").show()

# implicit inner join using two columns
testDF.join(odiDF, ["id", "year"]).show()

# explicit inner join
testDF.join(odiDF, testDF["id"] == odiDF["id"], "inner").show()

# explicit inner join using multiple columns
testDF.join(odiDF, ["id", "year"], "inner").show()
testDF.join(odiDF, (testDF["id"] == odiDF["id"]) & (testDF["year"] == odiDF["year"]), "inner").show()

# outer joins
testDF.join(odiDF, testDF["id"] == odiDF["id"], "left_outer").show()
testDF.join(odiDF, testDF["id"] == odiDF["id"], "right_outer").show()
testDF.join(odiDF, testDF["id"] == odiDF["id"], "full_outer").show()

# see the null values for some particular column
testDF.join(odiDF, ["id", "year"], "leftOuter").toDF(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies",
    "sfname", "omatches", "oinnings", "oruns", "ocenturies").where(col("omatches").isNull()).show()

# drop all null values
testDF.join(odiDF, ["id", "year"], "leftOuter") \
    .toDF(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies",
    "sfname", "omatches", "oinnings", "oruns", "ocenturies") \
    .na.drop().show()

# to filter out null values for a column
testDF.join(odiDF, ["id", "year"], "leftOuter") \
    .toDF("id", "year", "fname", "tmatches", "tinnings", "truns",
          "tcenturies", "sfname", "omatches", "oinnings", "oruns", "ocenturies") \
    .where(col('omatches').isNotNull()).show()

# isnull then 0 else value for a column with null values
testDF.join(odiDF, ["id", "year"], "leftOuter") \
    .toDF(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies", "sfname",
    "omatches", "oinnings", "oruns", "ocenturies") \
    .select(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies",
    when(isnull(col("omatches")), 0).otherwise(col("omatches")).alias("omtnull")) \
    .show()

# adding a computed column
testDF.join(odiDF, ["id", "year"], "leftOuter") \
    .toDF(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies", "sfname",
    "omatches", "oinnings", "oruns", "ocenturies") \
    .select(
    "id", "year", "fname", "tmatches", "tinnings", "truns", "tcenturies",
    when(isnull(col("omatches")), 0).otherwise(col("omatches")).alias("omtnull")) \
    .withColumn('totmatches', col('tmatches') + col('omtnull')) \
    .show()

# leftsemi join
testDF.join(odiDF, testDF["id"] == odiDF["id"], "left_semi").show()

# self join
testDF.alias("a").join(testDF.alias("b")) \
    .where(col('a.id') == col('b.id')) \
    .show()
testDF.alias("a").join(testDF.alias("b")) \
    .where((col('a.id') == col('b.id')) &
           (col('a.year') == col('b.year')) & (col('a.innings') > 10)) \
    .show()

# join, grouping and aggregation
testDF.groupBy("year") \
    .agg(sum("runs").alias("totruns"), sum("centuries").alias("totcents"),
         sumDistinct("centuries").alias("distcent")) \
    .show()
