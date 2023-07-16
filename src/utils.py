import time
import pyspark.rdd
import pyspark.sql.dataframe


def timeit(func, spark, *args, **kwargs):
    """
    Measure execution time of a function for multiple runs.
    
    Args:
        func: Function to be executed.
        spark: SparkSession instance.
        *args: Variable length argument list for the function.
        **kwargs: Arbitrary keyword arguments for the function.
    
    Returns:
        tuple: A tuple containing the list of execution times and the result of the last function call.
    """
    runs = 4 
    times = []

    for i in range(runs):
        start = time.time()
        result = func(*args, **kwargs)
        if isinstance(result, pyspark.rdd.RDD):
            result.persist()
            result.collect()
            result.unpersist()
        elif isinstance(result, pyspark.sql.dataframe.DataFrame):
            # If the result is a DataFrame, create a unique temp view for each run
            temp_view_name = "temp_view_%s" % i
            result.createOrReplaceTempView(temp_view_name)
            spark.sql("SELECT * FROM %s" % temp_view_name).show()
        end = time.time()
        times.append(end - start)

    return times, result


def calculate_average(times):
    """
    Calculate the average of a list of numbers.
    
    Args:
        times: List of numbers to calculate the average of.
    
    Returns:
        float: Average value.
    """
    total_time = sum(times)
    return total_time / len(times)


def calculate_error_margin(times, average):
    """
    Calculate the error margin for a list of numbers given the average.
    
    Args:
        times: List of numbers to calculate the error margin for.
        average: Average value.
    
    Returns:
        float: Error margin with a confidence interval of two standard
               deviations.
    """
    squared_diffs = [(time - average) ** 2 for time in times]
    variance = sum(squared_diffs) / (len(times) - 1)
    standard_deviation = variance ** 0.5
    return 2 * standard_deviation
