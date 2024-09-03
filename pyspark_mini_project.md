You will work with the futures and options end of day from nseindia.com.

The put call ratio is considered as a market direction indicator. Above 0.8 it ndicates bullishness and above 1.2 extreme bullishness. Likewise below 0.8 and 0.6 indicate bearishness and extreme bearishness respectively

Load the data from the s3 bucket folder located at s3a://databkt31/fod

Drop the extra column and convert the timestamp columns and expiry_dt columns to date columns

You will have to create this program and execute it using spark-submit

To modularize the functinality add the date converther helper function we have been using mnameToNo to a s3 location andd add that directory using the sc.addFile method, and ensure providing the recursive option

Filter the data to include only option_typ CE and aggregate the sum of open interest calling the aggregated column totce for the transformed data by symbol, converted expiry date column, converted date column

Repeat the above step for option_typ PE - group by same columns and call sum of open_int totpe

Join the two to add a pcr column as totpe / totce

write the analzyed, joined data partitioned by date in parquet and csv format to a target location in s3

write the analyzed, joined data to redshift to tables named fopcr<yourname>

provide the spark-submit command invocation to deploy the above program to a spark standalone cluster with the program taking four arguments - first one for the files to be added to modularize the program, second one for the source files, third one for the target parquet location, fourth one for the target csv location
