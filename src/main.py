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

# Import RDD joins
from joins import broadcast_join
from joins import repartition_join

# Import Optimiser script
from optimiser import use_optimiser

# Import csv-to-parquet converter
from csv_to_parquet import convert_csv_to_parquet


def part1():
    ###################### Task 1 ######################
    # Convert CSVs to Parquet; run only once. Should you
    # wish to repeat the process, comment it out.
    convert_csv_to_parquet()


    ################## Tasks 2, 3 & 4  ##################
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
        
        times[rdd_query_name]               = globals()[rdd_query_name](sc)
        times[parquet_query_name], _        = globals()[parquet_query_name](spark)
        times[csv_query_name], query_output = globals()[csv_query_name](spark)

        # Save the query-output, in dataframe format, on a text file 
        with open('../output/df_results/Q%sDF.txt' % i, 'w') as f:
            f.write(query_output)

        # Consistency in execution times
        spark.stop()
        sc.stop()
        print(times)

    # Compute execution times and write to a text file
    with open('../output/part_1_times.txt', 'w') as f:
        for query, execution_time in times.items():
            f.write('%s: %.2f seconds\n' % (query, execution_time))
    

def part2():
    ###################### Task 1 ######################
    times = {}

    spark = SparkSession \
                .builder \
                .appName("All-use session") \
                .getOrCreate() \
                .sparkContext

    times['Broadcast Join'], broadcast_result = broadcast_join(spark)
    times['Repartition Join'], _              = repartition_join(spark)

    # Compute execution times and write to a text file
    with open('../output/join_type_times.txt', 'w') as f:
        for query, execution_time in times.items():
            f.write('%s: %.2f seconds\n' % (query, execution_time))
            
    # Save the result to text files
    with open('../output/join_outputs.txt', 'w') as f:
        for result in broadcast_result:
            f.write(str(result) + '\n')

    # Consistency in execution times
    spark.stop()


    ###################### Task 2 ######################
    times = {}

    # Two instances are created since Spark tends to keep
    # metadata from each run in order to optimise reading
    # and calculating future queries.
    spark = SparkSession \
            .builder \
            .appName('Using Catalyst') \
            .getOrCreate()
    sc = spark

    times["Using Catalyst"], with_catalyst            = use_optimiser(spark)
    times["Without using Catalyst"], without_catalyst = use_optimiser(sc, disabled="Y")

    spark.stop()
    sc.stop()

    # Compute execution times and write to a text file
    with open('../output/catalyst_times.txt', 'w') as f:
        for query, execution_time in times.items():
            f.write('%s: %.2f seconds\n' % (query, execution_time[0]))
            
    # Save the optimised query plan to text file
    with open('../output/optimised_plan.txt', 'w') as f:
        f.write(with_catalyst)

    # Save the non-optimised query plan to text file 
    with open('../output/non_optimised_plan.txt', 'w') as f:
        f.write(without_catalyst)


if __name__ == "__main__":
    part1()
    part2()
