import io
import sys
import contextlib
from utils import timeit


def create_temp_tables(spark):
    df = spark.read.format("parquet")
    df1 = df.load("hdfs://master:9000/home/user/files/ratings.parquet") 
    df2 = df.load("hdfs://master:9000/home/user/files/movie_genres.parquet") 
    df1.registerTempTable("ratings") 
    df2.registerTempTable("genres")


def use_optimiser(spark, disabled = "N"):

   # Fetch relations
   create_temp_tables(spark)

   if disabled == "Y":
      spark.conf.set("spark.sql.cbo.enable", False)
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
   elif disabled == "N":
      pass 
   else:
      raise Exception ("This setting is not available.")

   query = """
           SELECT *
           FROM (SELECT * FROM genres LIMIT 100) AS g, ratings AS r
           WHERE r.mv_id = g.mv_id
      """

   stdout = io.StringIO()
   with contextlib.redirect_stdout(stdout):
        spark.sql(query).explain()

   # Get the captured standard output
   query_plan = stdout.getvalue()

   return timeit(spark.sql(query).show), query_plan
