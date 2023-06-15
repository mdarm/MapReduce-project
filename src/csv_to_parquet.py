from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType


def convert_csv_to_parquet():
    # Create spark instance
    spark = SparkSession \
            .builder \
            .appName("Add schemas to CSVs and make Parquet files") \
            .getOrCreate() 


    # Set schemas of csv files needed in Part 1 of the project
    movies_schema = StructType([
        StructField("mv_id", IntegerType()),
        StructField("name", StringType()),
        StructField("description", StringType()),
        StructField("year", IntegerType()),
        StructField("duration", IntegerType()),
        StructField("prod_cost", IntegerType()),
        StructField("revenue", IntegerType()),
        StructField("popularity", FloatType())
        ])

    ratings_schema = StructType([
        StructField("usr_id", IntegerType()),
        StructField("mv_id", IntegerType()),
        StructField("rating", FloatType()),
        StructField("time_stamp", IntegerType())
        ])

    movie_genres_schema = StructType([
        StructField("mv_id", IntegerType()),
        StructField("genre", StringType())
        ])

    # Set schemas of csv files need in Part 2 of the project
    employeesR_schema = StructType([
        StructField("id", IntegerType()),
        StructField("name", StringType()),
        StructField("dep_id", IntegerType())
        ])

    departmentsR_schema = StructType([
        StructField("dep_id", IntegerType()),
        StructField("dep_name", StringType())
        ])


    # Load the aforementioned csv files into dataframes 
    movies_df = spark.read.format('csv') \
            .options(header='false') \
            .schema(movies_schema) \
            .load("hdfs://master:9000/home/user/files/movies.csv")

    ratings_df = spark.read.format('csv') \
            .options(header='false') \
            .schema(ratings_schema) \
            .load("hdfs://master:9000/home/user/files/ratings.csv")

    movie_genres_df = spark.read.format('csv') \
            .options(header='false') \
            .schema(movie_genres_schema) \
            .load("hdfs://master:9000/home/user/files/movie_genres.csv")

    employeesR_df = spark.read.format('csv') \
            .options(header='false') \
            .schema(employeesR_schema) \
            .load("hdfs://master:9000/home/user/files/employeesR.csv")

    departmentsR_df = spark.read.format('csv') \
            .options(header='false') \
            .schema(departmentsR_schema) \
            .load("hdfs://master:9000/home/user/files/departmentsR.csv")


    # Save the dataframes as Parquet
    movies_df.write.parquet("hdfs://master:9000/home/user/files/movies.parquet")
    ratings_df.write.parquet("hdfs://master:9000/home/user/files/ratings.parquet")
    movie_genres_df.write.parquet("hdfs://master:9000/home/user/files/movie_genres.parquet")
    employeesR_df.write.parquet("hdfs://master:9000/home/user/files/employeesR.parquet")
    departmentsR_df.write.parquet("hdfs://master:9000/home/user/files/departmentsR.parquet")
