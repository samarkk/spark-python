from pyspark.sql.functions import *
from calendar import Calendar

def mnameToNo(dt):
    mname = dt[3:6].upper()
    calendar = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10",
                "NOV": "11", "DEC": "12"}
    return dt.upper().replace(mname, calendar[mname])

udf_mname_to_no = udf(mnameToNo)

def add_cols_and_group_df(input_df):
    output_df = input_df.drop("_c15"). \
        withColumn('rts', to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy")).\
        withColumn('YEAR', year(to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))).\
        withColumn('MONTH', month(to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))).\
        withColumn('DAY', dayofyear(to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy")))
    grouped_df = output_df.groupBy('rts', 'SYMBOL','INSTRUMENT','EXPIRY_DT','OPTION_TYP','YEAR', 'MONTH', 'DAY') \
        .agg(sum('contracts').alias('Contracts'),
             sum('VAL_INLAKH').alias('VALUE'),
             sum('OPEN_INT').alias('OPEN_INT'))
    return grouped_df

def group_df(input_df):
    output_df = input_df.drop("_c15"). \
        withColumn('rts', to_timestamp(udf_mname_to_no("TIMESTAMP"), "dd-MM-yyyy"))
    grouped_df = output_df.groupBy('rts', 'SYMBOL','INSTRUMENT','EXPIRY_DT','OPTION_TYP','YEAR', 'MONTH', 'DAY') \
        .agg(sum('contracts').alias('Contracts'),
             sum('VAL_INLAKH').alias('VALUE'),
             sum('OPEN_INT').alias('OPEN_INT'))
    return grouped_df

def add_pcr_to_df(input_df):
    return input_df \
        .filter("INSTRUMENT != 'FUTSTK'") \
        .groupBy('rts', 'SYMBOL', 'EXPIRY_DT', 'YEAR', 'MONTH', 'DAY') \
        .pivot('OPTION_TYP', ['XX','PE','CE']) \
        .agg(sum('Contracts').alias('contracts')) \
        .withColumn('pcr', col('PE')/col('CE'))

def combine_grouped_and_pcr_dfs(grouped_df, pcr_df):
    return grouped_df.alias('fgi').join(pcr_df.alias('pcr'),['rts', 'SYMBOL', 'EXPIRY_DT']) \
        .select('rts','SYMBOL', 'EXPIRY_DT', 'INSTRUMENT', 'OPTION_TYP', 'CONTRACTS', 'VALUE', 'OPEN_INT', 'PCR','fgi.YEAR', 'fgi.MONTH', 'fgi.DAY')

def write_processed_output(processed_df,output_path,output_mode,table_name):
    processed_df.write \
        .partitionBy('YEAR','MONTH', 'DAY') \
        .mode(output_mode) \
        .option('path', output_path) \
        .format('parquet') \
        .saveAsTable(table_name)


