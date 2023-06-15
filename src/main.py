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

# Import necessary functions
from utils import timeit, calculate_average, calculate_error_margin

def main():
    # Convert CSVs to Parquet
    #convert_csv_to_parquet()

    times = {}

    # Run queries and store execution times
    #times['rdd_query1'], _ = rdd_query1()
    #times['sql_parquet_query1'], _ = sql_parquet_query1()
    #times['sql_csv_query1'], _ = sql_csv_query1()

    # Calculate execution times for each query
    for i in range(1, 3):
        query_name = 'rdd_query%s' % (i)
        parquet_query_name = 'sql_parquet_query%s' % (i)
        times[query_name], _ = globals()[query_name]()
        times[parquet_query_name], _ = globals()[parquet_query_name]()

    # Compute averages and error margins and write to a text file
    with open('../output/times.txt', 'w') as f:
        for query, time_list in times.items():
            avg_time = calculate_average(time_list)
            error_margin = calculate_error_margin(time_list, avg_time)
            f.write('%s: %.4f with an uncertainty of %.4f seconds\n' % (query, avg_time, error_margin))


if __name__ == "__main__":
    main()
