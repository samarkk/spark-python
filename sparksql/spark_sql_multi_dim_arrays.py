import os
import sys
from sys import argv

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

spark = SparkSession.builder.appName('SparkXML').getOrCreate()

arrayArrayData = [
    ("James",[["Java","Scala","C++"],["Spark","Java"]]),
    ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
    ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

df.select(df.name,explode(df.subjects)).show(truncate=False)

# If you want to flatten the arrays, use flatten function which converts array of array columns to a single array on DataFrame
df.select(df.name,flatten(df.subjects)).show(truncate=False)

# https://docs.databricks.com/_static/notebooks/higher-order-functions.html

spark.sql(
    '''
    CREATE OR REPLACE TEMPORARY VIEW nested_data AS
SELECT   id AS key,
         ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT), CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT)) AS values
         ,
         ARRAY(ARRAY(CAST(RAND(1) * 100 AS INT), CAST(RAND(2) * 100 AS INT)), ARRAY(CAST(RAND(3) * 100 AS INT), CAST(RAND(4) * 100 AS INT), CAST(RAND(5) * 100 AS INT))) AS nested_values
FROM range(5)
    '''
)

'''
A simple example
Let's ground the concepts with a basic transformation. In this case, the higher order function, transform, will iterate over the array values, apply the associated lambda function to each element, and create a new array. The lambda function, element + 1, specifies how each element is manipulated. In SQL this will look like this:
'''
spark.sql(
    '''
    SELECT  key,
        values,
        TRANSFORM(values, value -> value + 1) AS values_plus_one
FROM    nested_data
    '''
).show(truncate=False)
"""
The transformation TRANSFORM(values, value -> value + 1) has two components:

TRANSFORM(values..) is the higher order function. This takes an array and an anonymous function as its input. Internally transform will take care of setting up a new array, applying the anonymous function to each element, and assigning the result to the output array.
The value -> value + 1 which is the anonymous function. The function is divided into two components separated by a -> symbol:
The argument list. In this case we only have one argument: value. We also support multiple arguments by creating a comma separated list of arguments enclosed by parenthesis, for example: (x, y) -> x + y.
The body. This is an expression that can use the arguments and outer variables to calculate the new value. In this case we add 1 to the value argument.
"""

"""
Capturing variables
You can also use other variables than the arguments in a lambda function; this is called capture. You can use variables defined on the top level, or variables defined in intermediate lambda functions. For example, the following transform adds the key (top level) variable to each element in the values array:
"""

spark.sql(
    '''
    SELECT  key,
        values,
        TRANSFORM(values, value -> value + key) AS values_plus_key
FROM    nested_data
    '''
).show()

'''
Nesting
If you want to transform deeply nested data, you can use nested lambda functions to do this. The following example transforms an array of integer arrays, and adds the key (top level) column and the size of the intermediate array to each element in the nested array.
'''

spark.sql(
    '''
    SELECT   key,
         nested_values,
         TRANSFORM(nested_values,
           values -> TRANSFORM(values,
             value -> value + key + SIZE(values))) AS new_nested_values
FROM     nested_data
    '''
).show(truncate=False)

'''
Supported functions
transform(array<T>, function<T, U>): array<U>
Transform an array by applying a function<T, U> to each element of an input array<T>.

The functional programming equivalent operation is map. This has been named transform in order to prevent confusion with the map expression (that creates a map from a key value expression).

The following query transforms the values array by adding the key value to each element:
'''

spark.sql(
    '''
    SELECT   key,
         values,
         TRANSFORM(values, value -> value + key) transformed_values
FROM     nested_data
    '''
).show(truncate=False)

'''
exists(array<T>, function<T, V, Boolean>): Boolean
Test whether a predicate function holds for any element in input array.

The following examples checks if the values array contains an elements for which the modulo 10 is equal to 1:
'''

spark.sql(
    '''
    SELECT   key,
         values,
         EXISTS(values, value -> value % 10 == 1) filtered_values
FROM     nested_data
    '''
).show(truncate=False)

'''
filter(array<T>, function<T, Boolean>): array<T>
Filter an output array<T> from an input array<T> by only only adding elements for which the predicate function<T, boolean> holds.

The following examples filters the values array such that only elements with a value > 50 are allowed:
'''

spark.sql(
    '''
    SELECT   key,
         values,
         FILTER(values, value -> value > 50) filtered_values
FROM     nested_data
    '''
).show(truncate=False)

'''
aggregate(array<T>, B, function<B, T, B>, function<B, R>): R
Reduce the elements of array<T> into a single value R by merging the elements into a buffer B using function<B, T, B> and by applying a finish function<B, R> on the final buffer. The initial value B is determined by a zero expression. The finalize function is optional, if you do not specify the function the finalize function the identity function (id -> id) is used.

This is the only higher order function that takes two lambda functions.

The following example sums (aggregates) the values array into a single (sum) value. Both a version with a finalize function (summed_values) and one without a finalize function summed_values_simple is shown:
'''

spark.sql(
    '''
    SELECT   key,
         values,
         REDUCE(values, 0, (value, acc) -> value + acc, acc -> acc) summed_values,
         REDUCE(values, 0, (value, acc) -> value + acc) summed_values_simple
FROM     nested_data
    '''
).show(truncate=False)

'''
You can also compute more complex aggregates. The code below shows the computation of the geometric mean of the array elements.
'''
spark.sql(
'''
SELECT   key,
values,
AGGREGATE(values,
          (1.0 AS product, 0 AS N),
          (buffer, value) -> (value * buffer.product, buffer.N + 1),
                             buffer -> Power(buffer.product, 1.0 / buffer.N)) geomean
FROM     nested_data
''').show(truncate=False)