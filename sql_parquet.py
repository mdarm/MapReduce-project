# sql_parquet_api.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list
from utils import timeit

spark = SparkSession \
    .builder \
    .appName("Dataframe API using Parquet files") \
    .getOrCreate()

# Fetch data
movies_df = spark.read.parquet("hdfs://master:9000/home/user/files/movies.parquet")
ratings_df = spark.read.parquet("hdfs://master:9000/home/user/files/ratings.parquet")
genres_df = spark.read.parquet("hdfs://master:9000/home/user/files/movie_genres.parquet")

# Create temporary relations
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("ratings")
genres_df.createOrReplaceTempView("genres")


def query1():
    
    query = """
        SELECT year, concat_ws(',', collect_list(cast((revenue - prod_cost) AS string))) AS profit
        FROM movies
        WHERE prod_cost > 0 AND revenue > 0 AND year > 1995
        GROUP BY year
        ORDER BY year
    """

    return timeit(spark.sql(query).show)
