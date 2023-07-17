# Import SparkSession
from pyspark.sql import SparkSession

# Import RDD queries 
from rdd import query1 as rdd_query1
from rdd import query2 as rdd_query2
from rdd import query3 as rdd_query3
from rdd import query4 as rdd_query4
from rdd import query5 as rdd_query5

# Import SQL-on-csv queries
from sql_csv import query1 as sql_csv_query1
from sql_csv import query2 as sql_csv_query2
from sql_csv import query3 as sql_csv_query3
from sql_csv import query4 as sql_csv_query4
from sql_csv import query5 as sql_csv_query5

# Import SQL-on-Parquet queries
from sql_parquet import query1 as sql_parquet_query1
from sql_parquet import query2 as sql_parquet_query2
from sql_parquet import query3 as sql_parquet_query3
from sql_parquet import query4 as sql_parquet_query4
from sql_parquet import query5 as sql_parquet_query5

# Import csv-to-parquet converter
from csv_to_parquet import convert_csv_to_parquet


def part_1():
    # Convert CSVs to Parquet (Task 1)
    #convert_csv_to_parquet()

    times = {}

    # Calculate execution times for each query (Tasks 2, 3 & 4)
    for i in range(1, 6):
        spark = SparkSession \
                .builder \
                .appName("All-use session") \
                .getOrCreate()
        sc = spark.sparkContext

        rdd_query_name     = 'rdd_query%s' % (i)
        parquet_query_name = 'sql_parquet_query%s' % (i)
        csv_query_name     = 'sql_csv_query%s' % (i)
        
        times[rdd_query_name], _     = globals()[rdd_query_name](sc)
        times[parquet_query_name], _ = globals()[parquet_query_name](spark)
        times[csv_query_name], _     = globals()[csv_query_name](spark)

        # Consistency in execution times
        spark.stop()
        sc.stop()
        print(times)

    # Compute execution times and write to a text file
    with open('../output/part_1_times.txt', 'w') as f:
        for query, execution_time in times.items():
            f.write('%s: %.2f seconds\n' % (query, execution_time))


if __name__ == "__main__":
    part_1()

