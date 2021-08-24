import sys
import os
from pyspark.sql.types import *

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# get spark libraries on the path and create the spark session
os.environ['PYLIB'] = os.environ['SPARK_HOME'] + '/python/lib'
sys.path.insert(0, os.environ['PYLIB'] + '/py4j-0.10.9-src.zip')
sys.path.insert(1, os.environ['PYLIB'] + '/pyspark.zip')

spark = SparkSession.builder.appName('Spark Dataframe Joins') \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel('WARN')
print('spark version {}, spark context version {}'.format(spark.version, sc.version))

mlist = ["""{"title": "Dangal", "country": "India", "year": 2016, "cast": {"Director": "Nitesh Tiwari", "LeadActor": "Aamir Khan", "BoxOfficeUSDMn": 330}, "genres": ["Biograhpy", "Drama"], "ratings": {"imdb": 8.4,"tomatoes": 4.55}}""", """{"title": "Fight Club", "country": "USA", "year": 1999, "cast": {"Director": "David Fincher", "LeadActor": "Brad Pitt", "BoxOfficeUSDMn": 104}, "genres": ["Action", "Drama"], "ratings": {"imdb": 8.8,"tomatoes": 4.46}}"""]

mrdd = sc.parallelize(mlist)
movieJSONDF = spark.read.json(mrdd)
movieJSONDF.show(2, False)

cmplxSchema = StructType([
    StructField("title", StringType(), True),
    StructField("country", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("cast",
                StructType([
                    StructField("Director", StringType(), True),
                    StructField("LeadActor", StringType(), True),
                    StructField("BoxOfficeUSDMn", DoubleType(), True)
                ]), True),
    StructField("genres", ArrayType(StringType(), True), True),
    StructField("ratings", MapType(StringType(), DoubleType()))])

movieJSONWithSchemaDF = spark.read.schema(cmplxSchema).json(mrdd)
movieJSONWithSchemaDF.show(2, False)
movieJSONWithSchemaDF.printSchema()

# querying complex types
# arrays with indexing
from pyspark.sql.functions import *
movieJSONWithSchemaDF.select(col("genres")[0], col("genres")[1]).show()
# structs using dot notation
movieJSONWithSchemaDF.select("cast.Director", "cast.LeadActor").show()
# maps using dot notations
movieJSONWithSchemaDF.select("ratings.imdb", "ratings.tomatoes").show()
# maps using keys
movieJSONWithSchemaDF.select(col("ratings")["imdb"], col("ratings")["tomatoes"]).show()

# filter using cmplex type part
movieJSONWithSchemaDF.filter("cast.LeadActor = 'Aamir Khan'").show(2, False)

# we can use the nested complex fields for grouping and aggregations
movieJSONWithSchemaDF.groupBy(col("cast.LeadActor"))\
    .agg(sum(col("ratings.imdb")).alias("imdbtotal"),
         sum(col("ratings.tomatoes")).alias("tomtot"))\
    .show()

# we can flatten the complex types using the explode function on arrays and maps
movieJSONWithSchemaDF.select(explode(col("genres"))).show()

# in one select clause we can have only one expansion. the line below will create an error
# movieJSONWithSchemaDF.select(explode(col("genres")), explode(col("ratings"))).show()(2, False)

# for multiple expansions we will have to do them sequentially
movieJSONWithSchemaDF\
    .select(explode(col("genres")), col("*"))\
    .select(explode(col("ratings")), col("*"))\
    .show(2, False)

# sql api for querying complex types
movieJSONWithSchemaDF.createOrReplaceTempView("mvcmplxtbl")
# struct parts using dot notation and genres exploded
spark.sql("""
select cast.director, cast.leadactor, ratings.imdb, 
ratings.tomatoes, explode(genres), 
*  from mvcmplxtbl""").show(2, False)

spark.sql("select cast.leadactor, sum(ratings.imdb) as imdbtot, sum(ratings.tomatoes) as tomtot from mvcmplxtbl group by cast.leadactor").show(2, False)

# we will use the get_json_object, from_json and to_json functions here to work with rdds of json strings
# we create mjsonDF as a dataframe of strings
mjsonDF = sc.parallelize(mlist).map(lambda x: (x,)).toDF(["mjson"])

# we can use get_json_object to navigate paths of a json string
mjsonDF.select(get_json_object(col("mjson"), "$.cast")).show(2, False)
mjsonDF.select(get_json_object(col("mjson"), "$.cast.Director")).show(2, False)

# we can use from_json along with a schema to load json and then use dot notation to
# access any path of the generated json
mjsonDF.select(from_json(col("mjson"), cmplxSchema)).show(2, False)
mjsonDF.select(from_json(col("mjson"), cmplxSchema).alias("mdet")).show(2, False)
mjsonDF.select(from_json(col("mjson"), cmplxSchema).alias("mdet")).select("mdet.*").show(2, False)

# finally we can call to_json to generate json data from the data frame
# here we take the movieJSONWithSchemaDF we had created to get back the json strings from which we had created the dataframe
movieJSONWithSchemaDF.select(to_json(struct(col("*"))).alias("moviestring")).show(2, False)

