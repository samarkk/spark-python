import os
import  sys
from sys import  argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

spark = SparkSession.builder.appName('Spark Log Processing') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

cm_data_location = argv[1]

cm_df = spark.read.option("inferSchema", True) \
    .option("header", True) \
    .csv(cm_data_location)

cm_df.show(2)

cmdf = cm_df.drop("_c13")
print("The cash market data frame schema")
cmdf.printSchema()

fo_data_location = argv[2]


fo_df = spark.read.option("inferSchema", True) \
    .option("header", True) \
    .csv(fo_data_location)
fodf = fo_df.drop("_c15").cache()

fodf.show(2, False)

# create a function to replace month names with numbers
def mnameToNo(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10",
                "NOV": "11", "DEC": "12"}
    return dt.upper().replace(mname, calendar[mname])

# verify that it works
mnameToNo('12-JAN-2019')

# create a udf from the month name to no function
# to apply to timestamp, expiry date colukns
udf_mname_to_no = udf(mnameToNo)

print("verifying that we our udf works and we can plug in to_timestamp")
fodf.select(to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy").alias('rts')).show(2)

# register a table against it
fodf.createOrReplaceTempView('fut_data')

# Analyses to be carried out
# Analysis 1 - Find by stock for a series the profitability status
# by day - who has the upper hand - buyers or sellers
# Metric to track that - rolling sum of the product of change in open interest
# and the average price at which the change took place

# we will generate these analyses using both the sql and the dataframe api
spark.udf.register("udf_mname_to_no", udf_mname_to_no)

print("Using sql to show overall profit loss status for f&o market")
spark.sql("""
select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%' and symbol = 'INFY' and expiry_dt = '26-Apr-2018'
order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")
""").show(100, False)

# from the final status obtained above subtract open interest * the closing price
# if the result is negative i.e buying amount - selling amount is negative, buyers were in the profit
# otherwise sellers were in profit

# now extend the analysis further
# and find out by symbol and expiry date the winners
# negative - buyers, positive - sellers
spark.sql("""
with pltbl as
( select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
 )
select *, b_s_pl_status - open_int * close as fpl_status 
from pltbl
where upper(expiry_dt) = timestamp
and symbol = 'TCS'
order by symbol, to_timestamp(udf_mname_to_no(expiry_dt), "dd-MM-yyyy")
""").show(1000, False)

spark.conf.set('spark.sql.shuffle.partitions', 4)
# save it as a dataframe
spark.sql("""
with pltbl as
( select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
 )
select *, b_s_pl_status - open_int * close as fpl_status 
from pltbl
where upper(expiry_dt) = timestamp
order by symbol, to_timestamp(udf_mname_to_no(expiry_dt), "dd-MM-yyyy")
""").repartition(1).write.mode('overwrite').save('fo_pl_details')

saved_fo_pl_df = spark.read.parquet('fo_pl_details')
saved_fo_pl_df.show()

spark.sql("""
with pltbl as
( select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
 )
select *, b_s_pl_status - open_int * close as fpl_status 
from pltbl
where upper(expiry_dt) = timestamp
order by symbol, to_timestamp(udf_mname_to_no(expiry_dt), "dd-MM-yyyy")
""").repartition(1).write.mode('overwrite').saveAsTable('fopltable')


spark.sql("""
with pltbl as
( select timestamp, symbol,expiry_dt,chg_in_oi,close,open_int,
sum(chg_in_oi)
over (partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy")) as cum_chg_oi,
sum(chg_in_oi*close)
over ( partition by symbol,expiry_dt, instrument order by to_timestamp(udf_mname_to_no(timestamp), "dd-MM-yyyy"))
as b_s_pl_status
from fut_data
where instrument like 'FUT%'
 )
select *, b_s_pl_status - open_int * close as fpl_status 
from pltbl
where upper(expiry_dt) = timestamp
order by symbol, to_timestamp(udf_mname_to_no(expiry_dt), "dd-MM-yyyy")
""").repartition(1).write.mode('overwrite').option('path','foplextbl').saveAsTable('fopltableex')


# carry out analysis 1 using dataframe api

# create a view with the proper timestamp column - tsp -timestamp proper
fodfvw = fodf.withColumn("tsp", to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))

partitionWindow = Window.partitionBy("symbol", "expiry_dt", "instrument").orderBy("tsp")

#then define the aggregation functions we are interested in over it
sumChgoi = sum("CHG_IN_OI").over(partitionWindow)
plStatus = sum(col('CHG_IN_OI') * col('close')).over(partitionWindow)


# and plug in the regular dataframe api
print("window functions executed using df api")
fodfvw.select( "timestamp", "symbol",
              "expiry_dt", "chg_in_oi", "close", "open_int",
              sumChgoi.alias("cum_chg_oi"), plStatus.alias("b_s_pl_status"))\
    .filter("symbol = 'INFY' and instrument like 'FUT%' and expiry_dt='26-Apr-2018'")\
    .orderBy(to_timestamp(udf_mname_to_no('timestamp'), "dd-MM-yyyy")).show(100, False)

# Analysis 2 - For the cash market find moving days average - 5, 20, 50, 200
# Prices crossing these measures are considered to be important signals, indicators

# create a proper timestamp column for the cash market data
cmdfvw = cmdf.withColumn(
    "tsp",
    to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))

cmdfvw.createOrReplaceTempView("cmdata")

print("verifying the extra column we added as a proper timestamp")
cmdfvw.limit(5).show()

# use sql to carry out window functions moving average using rows preceding
print("using sql to carry out window functions moving average using rows preceding")
spark.sql("""
select symbol, timestamp,close,
avg(close) over(partition by symbol order by tsp rows 5 preceding )as mv5avg,
avg(close) over(partition by symbol order by tsp rows 20 preceding )as mv20avg
from cmdata
where symbol = 'INFY'
order by tsp
""").show()

# using dataframe api to calculate moving average
print("using dataframe api to calculate moving average")
mvngAvgSpec = avg("close").over(
    Window.partitionBy("symbol").orderBy("tsp").rowsBetween(-5, 0))

cmdfvw.select("symbol", "timestamp", "tsp", "close",
              mvngAvgSpec.alias("mvng_avg_5")).filter(
    "symbol = 'INFY'").orderBy(
    "tsp").show()

# Analysis 3 - we want to carry out operations over series for symbols
# we'd like to correlate price on last thursday with ones 5, 10, 15, 20 days earlier
# We want sequnce number for the entries for a stock
# monotonically_increasing_id will not do that for us
# we can use row_number window function to do this

spark.sql("""
select symbol, timestamp, close,
row_number() over(partition by symbol order by tsp) as rno
from cmdata
where symbol in ('INFY', 'TCS')""").show()

print("dafaframe api showing row number")
rnospec = row_number().over(
    Window.partitionBy("symbol").orderBy(
        "tsp"))

cmdfvw.select("symbol", "timestamp", "close",
              rnospec.alias("rno")).filter(
    "symbol in ('INFY','TCS')").show()

