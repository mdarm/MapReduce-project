# rdd_api.py
from pyspark.sql import SparkSession
from utils import timeit

sc = SparkSession \
      .builder \
      .appName("RDD API") \
      .getOrCreate() \
      .sparkContext
    
movies_rdd   = sc.textFile("hdfs://master:9000/home/user/files/movies.csv")
genres_rdd   = sc.textFile("hdfs://master:9000/home/user/files/movie_genres.csv")
ratings_rdd  = sc.textFile("hdfs://master:9000/home/user/files/ratings.csv")


def query1():
    movies = movies_rdd.map(lambda row: row.split(',')) \
                       .filter(lambda att: len(att) > 7 and att[3].isdigit() and att[6].isdigit() and att[5].isdigit()) \
                       .map(lambda att: (int(att[3]), (int(att[6]), int(att[5])))) \
                       .filter(lambda att: att[0] > 1995 and att[1][0] > 0 and att[1][1] > 0) \
                       .map(lambda att: (att[0], str(att[1][0] - att[1][1]))) \
                       .reduceByKey(lambda v1, v2: v1 + ", " + v2) \
                       .sortBy(lambda pair: pair[0])
    
    return timeit(movies.collect)
