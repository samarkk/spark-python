import os
import sys
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')
os.environ['PYSPARK_SUBMIT_ARGS'] = \
    '--packages com.databricks:spark-xml_2.12:0.14.0 pyspark-shell'

spark = SparkSession.builder.appName('SparkXML').getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')

xml_file_location = 'D:/ufdata/books.xml'
df = spark.read.format('xml').options(rowTag='book').load(xml_file_location)
df.show(100, False)

xml_save_location = 'D:/ufdata/processed_xml.xml'
df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='book', rootTag='books') \
    .mode('overwrite') \
    .save(xml_save_location)

# read with a schema
customSchema = StructType([
    StructField("_id", StringType(), True),
    StructField("author", StringType(), True),
    StructField("description", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("publish_date", StringType(), True),
    StructField("title", StringType(), True)])

spark.read \
    .format('xml') \
    .options(rowTag='book') \
    .load(xml_file_location, schema=customSchema) \
    .show()

'''
Conversion from XML to DataFrame
Attributes: Attributes are converted as fields with the heading prefix, attributePrefix.

<one myOneAttrib="AAAA">
    <two>two</two>
    <three>three</three>
</one>
produces a schema below:

root
 |-- _myOneAttrib: string (nullable = true)
 |-- two: string (nullable = true)
 |-- three: string (nullable = true)
Value in an element that has no child elements but attributes: The value is put in a separate field, valueTag.

<one>
    <two myTwoAttrib="BBBBB">two</two>
    <three>three</three>
</one>
produces a schema below:

root
 |-- two: struct (nullable = true)
 |    |-- _VALUE: string (nullable = true)
 |    |-- _myTwoAttrib: string (nullable = true)
 |-- three: string (nullable = true)
'''

'''
Conversion from DataFrame to XML
Element as an array in an array: Writing a XML file from DataFrame having a field ArrayType with its element as ArrayType would have an additional nested field for the element. This would not happen in reading and writing XML data but writing a DataFrame read from other sources. Therefore, roundtrip in reading and writing XML files has the same structure but writing a DataFrame read from other sources is possible to have a different structure.

DataFrame with a schema below:

 |-- a: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: string (containsNull = true)
with data below:

+------------------------------------+
|                                   a|
+------------------------------------+
|[WrappedArray(aa), WrappedArray(bb)]|
+------------------------------------+
produces a XML file below:

<a>
    <item>aa</item>
</a>
<a>
    <item>bb</item>
</a>
'''