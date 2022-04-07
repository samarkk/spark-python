# pyspark launch with delta
# 1.0 is compatible with spark 3.1, 1.1 with spark 3.2
#pyspark --packages io.delta:delta-core_2.12:1.0.1 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --master yarn

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from stocks_function_helper import *


# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages io.delta:delta-core_2.12:1.0.1 pyspark-shell --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"'

spark = SparkSession.builder.appName('Avro Streaming') \
    .getOrCreate()
# spark.conf.set("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
# spark.conf.set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

# read the partitioned futures and options data
fo_data_location = '201819/fo'
fo_df = spark.read.option("inferSchema", True) \
    .option("header", True) \
    .option('mapred.input.dir.recursive', True) \
    .csv(fo_data_location)

# we are going to load the partitoned dataset
# we are going to drop the extra column
# add on a proper time stamp column and add on year, month, day columns
# we will group by symbol, instrumnent, expiry date, option type
# and find out the aggregate contracts and total value

# So for every date for every symbol we will have the aggregates for futures, calls and puts
# and all of these we will capture in a function in the functions helper notebook
# and call the functionality from there directly

# the function is def grouped_df(input_df) and it returns back the grouped df
fodf = fo_df.drop("_c15").withColumn('rts', to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))
# fodf.select(to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy").alias('rts')).show(2)
fodf.show(2)

fodf_grouped_instrument = add_cols_and_group_df(fo_df)
fodf_grouped_instrument.show(2)

fodf_pcr = add_pcr_to_df(fodf_grouped_instrument)

fodf_processed = combine_grouped_and_pcr_dfs(
    fodf_grouped_instrument, fodf_pcr)

spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
fodf_processed.write.saveAsTable('fopr')
# fodf_processed.write.mode('overwrite').insertInto('fopr')


# ################ delta  #############
# get the futures and options processed table
# do a show to verify data exists
fopr_reg = spark.read.table('fopr')
fopr_reg.show()
# for exploration filter fopr_reg to year 2018, month 1
fopr_reg.filter("year = 2018 and month = 1").show()
# just checking the count
fopr_reg.filter("year = 2018 and month = 1").count()

# save the filtered fo processed table as an external delta table in the mounted location
fopr_delta_location = '/user/vagrant/foprdelta'
fopr_reg.filter("year = 2018 and month = 1") \
    .write.format('delta') \
    .option('path', fopr_delta_location) \
    .saveAsTable('foprdel')

# read from the directory location directly
spark.read.format('delta').load(fopr_delta_location).show()

# save jan to mar 2018 processed data as a partitioned delta table
fopr_reg.filter("year = 2018 and month between 1 and 3").write.format('delta') \
    .option('path', '/user/vagrant/foprdelpart') \
    .partitionBy('year', 'month', 'day').saveAsTable('foprdelpart')

spark.sql("select * from foprdel where year = 2018 and month = 1 and symbol = 'ACC'").show()

# -- update the foprdel table
spark.sql("""
update foprdel
set symbol = 'ACCEMENT'
where year = 2018 and month = 1 and symbol = 'ACC'
""")

spark.sql("select * from foprdel where year = 2018 and month = 1 and symbol = 'ACCEMENT'").show()

# -- insert back ACC rows into foprdel table
spark.sql("""
insert into foprdel
select * from fopr where year = 2018 and month = 1 and symbol = 'ACC'
""")

# -- as changes are made versions are saved
# -- check the history of the delta table
spark.sql('DESC HISTORY foprdel').show()
spark.sql('DESC HISTORY foprdel').select(
    "version", "timestamp", "operation", "operationParameters").show(100, False)
# -- time travel to any version adding on the clause VERSION AS OF
spark.read.format("delta").option("versionAsOf", 1).load('/user/vagrant/foprdelta').show()
spark.read.format("delta").option("versionAsOf", 1).load('/user/vagrant/foprdelta') \
    .filter("symbol='ACCEMENT'").show(100, False)
# ACC should not be there in version 0 as it was updated to ACCEMENT
spark.read.format("delta").option("versionAsOf", 0).load('/user/vagrant/foprdelta') \
    .filter("symbol='ACCEMENT'").show(100, False)

# sql VERSION is available only on Databricks
# spark.sql("""
# SELECT * FROM foprdel VERSION AS OF 1
# """).show()

# time ttavel using the timestamp for version 2
# where data for ACC for month 1 should be available

# spark.sql("SELECT * FROM FOPRDEL TIMESTAMP AS OF '2022-04-02 04:11:42.622").show(100, False)
# timestamp as of in sql not supported in open source version

# verify that the corresponding timestamp versions have ACCEMENT and ACC respecitvely
spark.read.format("delta"). \
    option("timestampAsOf", "2022-04-02 04:02:24.172"). \
    load('/user/vagrant/foprdelta') \
    .filter("symbol='ACC'").count()

spark.read.format("delta"). \
    option("timestampAsOf", "2022-04-02 04:02:24.172"). \
    load('/user/vagrant/foprdelta') \
    .filter("symbol='ACCEMENT'").count()

# verify that after ACC insert back there are records for both ACC and ACCEMENT
spark.read.format("delta"). \
    option("timestampAsOf", "2022-04-02 04:11:42.622"). \
    load('/user/vagrant/foprdelta') \
    .filter("symbol like 'ACC%'") \
    .groupBy(col("symbol")).count().show(100, False)

# Delete ACC data from the table
spark.sql("DELETE from foprdel  where symbol = 'ACC'")

# -- there may be GDBR or California Data Protection Requirements
# -- we can vaccuum the table - which by default works with a retention period of 168 hours
# -- force immediate vacuuming by setting retain time 0 hours and adding on the configuration shown below
spark.sql('VACUUM foprdel RETAIN 0 HOURS')
# pyspark.sql.utils.IllegalArgumentException: requirement failed: Are you sure you would like to vacuum files with such a low retention period? If you have
# writers that are currently writing to this table, there is a risk that you may corrupt the
# state of your Delta table.
#
# If you are certain that there are no operations being performed on this table, such as
# insert/upsert/delete/optimize, then you may turn off this check by setting:
# spark.databricks.delta.retentionDurationCheck.enabled = false
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", False)

'''
To time travel to a previous version, you must retain both the log and the data files for that version.

The data files backing a Delta table are never deleted automatically; data files are deleted only when you run VACUUM. VACUUM does not delete Delta log files; log files are automatically cleaned up after checkpoints are written.

By default you can time travel to a Delta table up to 30 days old unless you have:

Run VACUUM on your Delta table.

Changed the data or log file retention periods using the following table properties:

delta.logRetentionDuration = "interval <interval>": controls how long the history for a table is kept. The default is interval 30 days.

Each time a checkpoint is written, Databricks automatically cleans up log entries older than the retention interval. If you set this config to a large enough value, many log entries are retained. This should not impact performance as operations against the log are constant time. Operations on history are parallel but will become more expensive as the log size increases.

delta.deletedFileRetentionDuration = "interval <interval>": controls how long ago a file must have been deleted before being a candidate for VACUUM. The default is interval 7 days.

To access 30 days of historical data even if you run VACUUM on the Delta table, set delta.deletedFileRetentionDuration = "interval 30 days". This setting may cause your storage costs to go up
'''

# delete rows for a symbol and insert them in the demonstration of the merge operation
spark.sql("DELETE from foprdel  where symbol = 'ITC'")
# merge sql not available in open source version
spark.sql(
"""
MERGE INTO FOPRDEL tgt
USING FOPRDEL VERSION AS OF 3 src
ON src.year = tgt.year and src.month = tgt.month and src.day = tgt.day and src.symbol = tgt.symbol and src.rts = tgt.rts and src.expiryb_dt = tgt.expiry_dt
WHEN NOT MATCHED THEN
INSERT *
""")

# load the delta table using DeltaTable form delta.tables
from delta.tables import  *
fdtbl = DeltaTable.forName(spark,'foprdel')
# verify that the count for ITC is 0
fdtbl.toDF().filter("symbol='ITC'").count()

# in a previous version we have data for the dropped symbol available
versionNoWithSymbol = 3

df_with_symbol = spark.read.format("delta").option("versionAsOf", versionNoWithSymbol).load('/user/vagrant/foprdelta')
# verify that there are records for the symbol in this prior version
df_with_symbol.filter("symbol = 'ITC'").count()

# merge the previous dataframe into the delta table using the whenNotMatchedInsert condition
fdtbl.alias("src").merge(
    df_with_symbol.alias("tgt"),
    "src.year = tgt.year and src.month = tgt.month and src.day = tgt.day and src.symbol = tgt.symbol and src.rts = tgt.rts and src.expiry_dt = tgt.expiry_dt"
).whenNotMatchedInsert(values={
    "rts": "tgt.rts",
    "SYMBOL": "tgt.SYMBOL",
    "EXPIRY_DT":"tgt.EXPIRY_DT",
    "INSTRUMENT":"tgt.INSTRUMENT",
    "OPTION_TYP":"tgt.OPTION_TYP",
    "CONTRACTS":"tgt.CONTRACTS",
    "VALUE":"tgt.VALUE",
    "OPEN_INT":"tgt.OPEN_INT",
    "PCR":"tgt.PCR",
    "YEAR":"tgt.YEAR",
    "MONTH":"tgt.MONTH",
    "DAY":"tgt.DAY"
}).execute()
# verify that the delta table after the merge has the records for the symbol
fdtbl.toDF().filter("symbol='ITC'").count()