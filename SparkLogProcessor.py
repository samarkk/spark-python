import os
import sys
import re
import datetime
import time

from pyspark.sql import SparkSession
from pyspark.sql import Row

from pyspark import  StorageLevel

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

spark = SparkSession.builder.appName('Spark Log Processing') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

# make provisions for processing the logs
# use a regex to split the line into the nine groups and map it to a sql row of nine columns

# set the log pattern to be used
APACHE_ACCESS_LOG_PATTERN = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)\s*(\S*)\s*" (\d{3}) (\S+)'

# a dictionary to replace month names with numbers
month_map = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6, 'Jul': 7,
             'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}


def parse_apache_time(s):
    """ Convert Apache time format into a Python datetime object
    Args:
        s (str): date and time in Apache time format
    Returns:
        datetime: datetime object (ignore timezone for now)
    """
    return datetime.datetime(int(s[7:11]),
                             month_map[s[3:6]],
                             int(s[0:2]),
                             int(s[12:14]),
                             int(s[15:17]),
                             int(s[18:20]))


def parseApacheLogLine(logline):
    """ Parse a line in the Apache Common Log format
    Args:
        logline (str): a line of text in the Apache Common Log format
    Returns:
        tuple: either a dictionary containing the parts of the Apache Access Log and 1,
               or the original invalid log line and 0
    """
    match = re.search(APACHE_ACCESS_LOG_PATTERN, logline)
    if match is None:
        return (logline, 0)
    size_field = match.group(9)
    if size_field == '-':
        size = int(0)
    else:
        size = int(match.group(9))
    return (Row(
        host=match.group(1),
        client_identd=match.group(2),
        user_id=match.group(3),
        date_time=parse_apache_time(match.group(4)),
        method=match.group(5),
        endpoint=match.group(6),
        protocol=match.group(7),
        response_code=int(match.group(8)),
        content_size=size
    ), 1)


# read the log file and use the mapped tuple second part to get a count of successfully parsed
# and failed logs

def parseLogs(fileLoc):
    """ Read and parse log file """
    parsed_logs = (sc
                   .textFile(fileLoc)
                   .map(parseApacheLogLine)
                   .cache()) \

    access_logs = (parsed_logs
                   .filter(lambda s: s[1] == 1)
                   .map(lambda s: s[0])
                   .cache()) \

    failed_logs = (parsed_logs
                   .filter(lambda s: s[1] == 0)
                   .map(lambda s: s[0]))
    failed_logs_count = failed_logs.count() \

    if failed_logs_count > 0:
        print('Number of invalid logline: %d' % failed_logs.count())
        for line in failed_logs.take(20):
            print('Invalid logline: %s' % line) \

    print('Read %d lines, successfully parsed %d lines, failed to parse %d lines' %
          (parsed_logs.count(), access_logs.count(), failed_logs.count()))
    return parsed_logs, access_logs, failed_logs


fileloc = "file:///D:/ufdata/apachelogs"
parsed_logs, access_logs, failed_logs = parseLogs(fileloc)

# confirm that all logs are loaded
assert (failed_logs.count() == 0)
assert (parsed_logs.count() == 1043177)
assert (access_logs.count() == parsed_logs.count())

# Calculate statistics based on the content size.
content_sizes = access_logs.map(lambda log: log.content_size).cache()
print('Content Size Avg: %i, Min: %i, Max: %s' % (
    content_sizes.reduce(lambda a, b: a + b) / content_sizes.count(),
    content_sizes.min(),
    content_sizes.max()))

# Response code analysis
# Response Code to Count
responseCodeToCount = (access_logs
                       .map(lambda log: (log.response_code, 1))
                       .reduceByKey(lambda a, b: a + b)
                       .cache())
responseCodeToCountList = responseCodeToCount.collect()
print('Found %d response codes' % len(responseCodeToCountList))
print('Response Code Counts: %s' % responseCodeToCountList)
assert len(responseCodeToCountList) == 7
assert sorted(responseCodeToCountList) == [(200, 940847), (302, 16244), (304, 79824),
                                           (403, 58), (404, 6185), (500, 2), (501, 17)]


# ##############################################
# check storage , persistence
# ##############################################

# cache is same as StorageLevel.MEMORY_ONLY
print(parsed_logs.getStorageLevel())

# persist to disk
parsed_logs_disk_persisted = parsed_logs.map(lambda x: x).persist(StorageLevel.DISK_ONLY)
parsed_logs_disk_persisted.count()
print(parsed_logs_disk_persisted.getStorageLevel())

# average requests per host per day
# find number of hosts by day - map to a tuple of host and day - and reduce by key
# (google, 1), (google, 2), (google, 1), (yahoo, 1), (yahoo, 2), (yahoo, 1), (yahoo, 1), (yahoo, 1)
# distinct - ((google, 1), (google, 2), (yahoo, 1), (yahoo, 2))
# (1, 1), (2, 1), (1, 1), (2, 1)
daily_hosts = access_logs\
    .map(lambda r: (r.date_time.day, r.host)) \
    .distinct()\
    .map(lambda x: (x[0], 1))\
    .reduceByKey(lambda x, y: x + y)

# find requests per day
# (google, 1), (google, 2), (google, 1), (yahoo, 1), (yahoo, 2), (yahoo, 1), (yahoo, 1), (yahoo, 1)
# (1, 1),      (2, 1),      (1, 1)     , (2, 1)    , (2, 1)    , (2, 1)    ,  (1, 1)    , (1, 1)
daily_requests = access_logs \
    .map(lambda r : (r.date_time.day, 1)) \
    .reduceByKey(lambda x, y: x + y)

# join daily hosts and aaily requests to find the average number of requests per host
print(daily_hosts \
    .join(daily_requests) \
    .map(lambda x: (x[0], x[1][0], x[1][1], x[1][1] / x[1][0])) \
    .sortBy(lambda x: x[0]) \
    .collect())

# time.sleep(3600)

