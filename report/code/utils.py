import io
import time
import contextlib


def timeit(func, *args, **kwargs):
    """
    Measure execution time of a function for a single run.
    
    Args:
        func: Function to be executed.
        *args: Variable length argument list for the function.
        **kwargs: Arbitrary keyword arguments for the function.
    
    Returns:
        tuple: A tuple containing the execution time and the result of the function call.
    """
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    execution_time = end - start

    return execution_time, result


def save_dataframe_output(dataframe):
    """
    Function to capture and return the complete output of a PySpark DataFrame.

    Args:
        dataframe (pyspark.sql.DataFrame): The DataFrame whose output is to be captured.

    Returns:
        str: The entire output of the DataFrame as a string.
    """
    
    # Calculate the number of rows in the DataFrame
    row_count = dataframe.count()

    # Create a StringIO object
    stdout = io.StringIO()

    # Execute show() on DataFrame and capture the output
    with contextlib.redirect_stdout(stdout):
        dataframe.show(n=row_count, truncate=False)

    # Get the captured standard output
    return stdout.getvalue()
