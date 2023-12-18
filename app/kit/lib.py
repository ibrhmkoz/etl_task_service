import time
from contextlib import contextmanager


def pipe(*functions):
    def piped_function(*args, **kwargs):
        result = args
        for function in functions:
            if isinstance(result, tuple):
                result = function(*result, **kwargs)
            else:
                result = function(result, **kwargs)
        return result

    return piped_function


@contextmanager
def time_block(label):
    start_time = time.time()
    try:
        yield
    finally:
        end_time = time.time()
        print(f"Time taken for '{label}': {end_time - start_time} seconds")


def retry(callback, times):
    def wrapper():
        attempts = 0
        while attempts <= times:
            try:
                return callback()
            except Exception as e:
                attempts += 1
                if attempts > times:
                    raise
        raise RuntimeError(f"Operation failed after {times} retries.")
    return wrapper
