import time

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
