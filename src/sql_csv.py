from pyspark.sql.functions import collect_list
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from utils import timeit, save_dataframe_output


# Set schemas of csv files
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


def create_temp_tables(spark):
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

    # Create temporary tables
    movies_df.createOrReplaceTempView("movies")
    ratings_df.createOrReplaceTempView("ratings")
    movie_genres_df.createOrReplaceTempView("genres")


def query1(spark):
    # Fetch relations
    create_temp_tables(spark)

    # Get the difference betwen revenue and production cost (i.e. profits) of every movie after 1995
    query = """
        SELECT year, concat_ws(',', collect_list(cast((revenue - prod_cost) AS string))) AS profit
        FROM movies
        WHERE prod_cost > 0 AND revenue > 0 AND year > 1995
        GROUP BY year
        ORDER BY year
   """
    
    execution_time, _ = timeit(spark.sql(query).show)
    query_output = save_dataframe_output(spark.sql(query))

    return execution_time, query_output


def query2(spark):
    # Fetch relations
    create_temp_tables(spark)

    # Get the movie id, the average rating and the total number of ratings for the movie “Cesare deve morire”
    query = """
        SELECT m.mv_id, COUNT(r.usr_id) AS user_count, AVG(r.rating) AS average_rating
        FROM movies AS m
        JOIN ratings AS r ON m.mv_id = r.mv_id
        WHERE m.name = 'Cesare deve morire'
        GROUP BY m.mv_id
    """    

    execution_time, _ = timeit(spark.sql(query).show)
    query_output = save_dataframe_output(spark.sql(query))

    return execution_time, query_output


def query3(spark):
    # Fetch relations
    create_temp_tables(spark)

    # Get the best Animation movie in terms of revenue for 1995
    query = """
        SELECT m.name AS movie_name, m.revenue AS revenue
        FROM movies AS m
        JOIN genres AS mg ON m.mv_id = mg.mv_id
        WHERE mg.genre = 'Animation' AND m.year = 1995 AND m.revenue > 0
        ORDER BY m.revenue DESC
        LIMIT 1
    """ 
    execution_time, _ = timeit(spark.sql(query).show)
    query_output = save_dataframe_output(spark.sql(query))

    return execution_time, query_output


def query4(spark):
    # Fetch relations
    create_temp_tables(spark)

    # Get the most popular Comedy movie for each year after 1995
    query = """
        WITH ranked_movies AS (
            SELECT m.year, m.name, m.popularity,
            ROW_NUMBER() OVER(PARTITION BY m.year ORDER BY m.popularity DESC) AS rank
            FROM movies AS m
            JOIN genres AS mg ON m.mv_id = mg.mv_id
            WHERE mg.genre = 'Comedy' AND m.year > 1995 AND m.popularity > 0 AND m.revenue > 0
            )
        SELECT year, name, popularity
        FROM ranked_movies
        WHERE rank = 1
        ORDER BY year
    """  

    execution_time, _ = timeit(spark.sql(query).show)
    query_output = save_dataframe_output(spark.sql(query))

    return execution_time, query_output


def query5(spark):
    # Fetch relations
    create_temp_tables(spark)

    # Get the average revenue for each year
    query = """
        SELECT year, AVG(revenue) AS avg_revenue
        FROM movies
        WHERE year > 0 AND revenue > 0
        GROUP BY year
        ORDER BY year DESC
    """    

    execution_time, _ = timeit(spark.sql(query).show)
    query_output = save_dataframe_output(spark.sql(query))

    return execution_time, query_output
