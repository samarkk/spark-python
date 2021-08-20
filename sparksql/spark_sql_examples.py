import os
import  sys
from sys import  argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

spark = SparkSession.builder.appName('Spark SQL Examples') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

ardd = sc.parallelize([('del', 'hot', 40), ('blr', 'cool', 30)])

# toDF function is available to convert RDDs to DataFrames
# It expects a rdd of tuples
# if there is a single element as in
# sc.parallelize([1,2,3,4]).toDF(['nos']).show()
# we will have an error
# this below will work all right
sc.parallelize([1,2,3,4]).map(lambda x: (x,)).toDF(['nos']).show()

adf = ardd.toDF(['city', 'weather', 'temp'])

adf.show()

acTransList = ["SB10001,1000", "SB10002,1200", "SB10003,8000",
               "SB10004,400", "SB10005,300", "SB10006,10000", "SB10007,500",
               "SB10008,56", "SB10009,30", "SB10010,7000",
               "CR10001,7000", "SB10002,-10"]
acRDD = sc.parallelize(acTransList)

acTransDF = acRDD.map(lambda x: x.split(',')).toDF(['accNo', 'tranAmount'])
acTransDF.show()

# Using create data frame method of spark

acTransDFFmSQLC = spark.createDataFrame(acRDD.map(lambda x: x.split(',')))\
    .toDF('accno', 'tranamount')
print(acTransDFFmSQLC.printSchema())
acTransDFFmSQLC.show()


print("\nData frame select column, columns")
# def select(col: String, cols: String*): DataFrame
print(acTransDF.select("accNo").collect())
acTransDF.show()

print("Data fame selection using select expr")
acTransDF.selectExpr("accNo as account_no", "tranAmount").show()

print("filter equivalent to where, operation - filter and where seem to be equivalent")
acTransDF.filter("tranAmount >= 1000").show()

acTransDF.where("tranAmount >= 1000 and accNo like'SB%' ").show()

print("Filtering for conditions on multiple columns")
acTransDF.filter((acTransDF.tranAmount.cast('float') >= 1000) &
                 (acTransDF.accNo.startswith('SB'))).show()

# creating user defined functions to apply to dataframe columns
tenf = lambda x: x * 10
tenudf = udf(tenf)
acTransDF.select('accNo','tranAmount', tenudf(acTransDF.tranAmount.cast('float')).alias('tranM10')).show()
acTransDF.select('accNo','tranAmount', tenudf(acTransDF['tranAmount'].cast('float')).alias('tranM10')).show()

# creating dataframes with a self defined schema

# a list of transactions - symbol, date, quantity, value
stlist = [
    "INFY,2017-05-01,2000,2164550",
    "INFY,2017-5-02,1954,2174352",
    "INFY,2017-06-03,2341,2934231",
    "INFY,2017-06-04,1814,1904557",
    "SBIN,2017-05-01,200061,3164550",
    "SBIN,2017-5-02,211954,3174352",
    "SBIN,2017-06-03,222341,3434234",
    "SBIN,2017-06-04,301814,4590455"]

# create a schema - which is a StructType that takes collection of
# column definitions as a single argument
stock_schema = StructType(
    [StructField('symbol', StringType()), StructField('trdate', StringType()),
     StructField('qty', IntegerType()), StructField('vlu', DoubleType())])
stock_rdd = sc.parallelize(stlist).map(lambda x: x.split(',')).map(
    lambda x: (x[0], x[1], int(x[2]), float(x[3])))
stdf = spark.createDataFrame(stock_rdd, stock_schema)
print(stdf.printSchema())
stdf.show()

# add on month and year column to enable aggregations by those levels
stdf_wym = stdf.select("symbol", year(to_date("trdate")).alias("yr"),
                       month(to_date("trdate")).alias("mnth"), "qty", "vlu")
stdf_wym.show()

print('Manipulating spark data frames using the underlying rdd')
stdf.rdd.map(lambda x: (x[0], x[1], x[2], x[3])).collect()

print("cube will provide every cobmination of the fields used to create the cube")
stdf_wym.cube("symbol", "yr", "mnth").agg(sum("qty").alias("qty"), sum("vlu").alias("vlu")).show()

print("\nrollup will rollup aggregates beginning from the first field in the rollup columns")
stdf_wym.rollup("symbol", "yr", "mnth").agg(sum("qty").alias("qty"), sum("vlu").alias("vlu")).show()

stdf_wym.createOrReplaceTempView("stmdtbl")
# saving the table to storage
stdf_file_loc_to_save_to = argv[1]
stdf_wym.write.mode('overwrite').parquet(stdf_file_loc_to_save_to)

print("Using sql to carry out multi dimensional aggregations")
spark.sql("""select symbol,yr,mnth,sum(qty) as qty, sum(vlu) as vlu
          from stmdtbl
          group by symbol, yr, mnth
          with cube""").show()

# grouping sets is only available with sql
# it provides the flexibility to choose the dimension combinations
# of interest
# if we have 3 columns, we have eight combinations
# with ten 1024, with 12 - 4096, with 15 - 32768 and with 20 - one million
spark.sql("""select symbol,yr,mnth,sum(qty) as qty, sum(vlu) as vlu
          from stmdtbl
          group by symbol, yr, mnth
          grouping sets ((symbol, yr), (symbol, mnth), (symbol))""").show()