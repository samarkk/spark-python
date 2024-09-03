You will work with the futures and options end of day from nseindia.com.

The put call ratio is considered as a market direction indicator. Above 0.8 it ndicates bullishness and above 1.2 extreme bullishness. Likewise below 0.8 and 0.6 indicate bearishness and extreme bearishness respectively

create a source bucket where you will add the files from s3://databkt31/fod location

create a lambda function which will be triggered when the file is added to the source bucket

create a destination bucket where the analzyed data will be stored by the glue job that will be invoked through the triggered lambda function

In the lambda function generate the source and destination locations. The destination location should have a partitioned structure with directories named dt=yyyy-mm-dd eg fo01JUN2023bhav.csv.gz analyzed data should go to dt=2023-06-01 folder

create a glue job which will be invoked by the lambda function using boto3

the glue job will take two arguments src and dest which will be generated in the lambda function

in the glue job carry out the steps to generate the output as per the steps below

Drop the extra column and convert the timestamp columns and expiry_dt columns to date columns

You can use the moudlarization technique developed in the pyspark exercise to get the mnameToNo function or plug it inline

Filter the data to include only option_typ CE and aggregate the sum of open interest calling the aggregated column totce for the transformed data by symbol, converted expiry date column, converted date column

Repeat the above step for option_typ PE - group by same columns and call sum of open_int totpe

Join the two to add a pcr column as totpe / totce

write the analzyed, joined data to the target location

add a file to the source location and verify that the job is invoked through the triggered lambda function and the correct output written to the target bucket
