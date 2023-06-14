# utils.py
import time

def timeit(func, *args, **kwargs):
    """Measure execution time of a function"""
    start = time.time()
    result = func(*args, **kwargs)
    end = time.time()
    execution_time = end - start
    print('Execution time: %f seconds' % (execution_time))

    return execution_time, result
