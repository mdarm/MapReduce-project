# main.py
from rdd import query1 as rdd_query1
from sql_parquet import query1 as sql_parquet_query1
from sql_csv import query1 as sql_csv_query1
# Import other queries...
from csv_to_parquet import convert_csv_to_parquet

def main():
    # Convert CSVs to Parquet
    #convert_csv_to_parquet()

    times = {}

    # Run queries and store execution times
    times['rdd_query1'], _ = rdd_query1()
    times['sql_parquet_query1'], _ = sql_parquet_query1()
    times['sql_csv_query1'], _ = sql_csv_query1()

    # Write times to a text file
    with open('times.txt', 'w') as f:
        for query, time in times.items():
            f.write("%s: %f seconds\n" % ((query), (time)))


if __name__ == "__main__":
    main()
