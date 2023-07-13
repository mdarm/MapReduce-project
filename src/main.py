# Import RDD queries and SparkSession instance
from rdd import query1 as rdd_query1
from rdd import query2 as rdd_query2
from rdd import query3 as rdd_query3
from rdd import query4 as rdd_query4
from rdd import query5 as rdd_query5
from rdd import spark as rdd_spark

# Import SQL-on-csv queries and SparkSession instance
from sql_csv import query1 as sql_csv_query1
from sql_csv import query2 as sql_csv_query2
from sql_csv import query3 as sql_csv_query3
from sql_csv import query4 as sql_csv_query4
from sql_csv import query5 as sql_csv_query5
from sql_csv import spark as csv_spark

# Import SQL-on-Parquet queries and SparkSession instance
from sql_parquet import query1 as sql_parquet_query1
from sql_parquet import query2 as sql_parquet_query2
from sql_parquet import query3 as sql_parquet_query3
from sql_parquet import query4 as sql_parquet_query4
from sql_parquet import query5 as sql_parquet_query5
from sql_parquet import spark as parquet_spark

# Import csv-to-parquet converter
from csv_to_parquet import convert_csv_to_parquet

# Import necessary functions
from utils import timeit, calculate_average, calculate_error_margin

def part_1():
    # Convert CSVs to Parquet (Task 1)
    #convert_csv_to_parquet()

    times = {}

    # Calculate execution times for each query (Tasks 2, 3 & 4)
    for i in [1, 2, 4, 5]: 
        rdd_query_name = 'rdd_query%s' % (i)
        parquet_query_name = 'sql_parquet_query%s' % (i)
        csv_query_name = 'sql_csv_query%s' % (i)
        rdd_query = globals()[rdd_query_name]
        parquet_query = globals()[parquet_query_name]
        csv_query = globals()[csv_query_name]
        times[rdd_query_name], _ = timeit(rdd_query, rdd_spark)
        times[parquet_query_name], _ = timeit(parquet_query, parquet_spark)
        times[csv_query_name], _ = timeit(csv_query, csv_spark)
        print(times)

    # Compute averages and error margins and write to a text file
    with open('../output/part_1_times.txt', 'w') as f:
        for query, time_list in times.items():
            avg_time = calculate_average(time_list)
            error_margin = calculate_error_margin(time_list, avg_time)
            f.write('%s: %.4f (+-%.4f) seconds\n' % (query, avg_time, error_margin))


if __name__ == "__main__":
    part_1()
