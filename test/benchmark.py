import time
import functools


class Benchmark:
    @staticmethod
    def timeit(func):
        """
        A decorator that benchmarks the strategy time of a function.
        """

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Benchmark: {func.__name__} executed in {elapsed_time:.6f} seconds")
            return result

        return wrapper

    @staticmethod
    def repeat(n: int):
        """
        A decorator that runs a function `n` times and prints the average strategy time.
        """

        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                total_time = 0
                result = None
                for _ in range(n):
                    start_time = time.time()
                    result = func(*args, **kwargs)
                    end_time = time.time()
                    total_time += (end_time - start_time)
                avg_time = total_time / n
                print(f"Benchmark: {func.__name__} executed {n} times, "
                      f"average time: {avg_time:.6f} seconds")
                return result

            return wrapper

        return decorator

    @staticmethod
    def profile_memory(func):
        """
        A decorator that prints memory usage for a function strategy.
        """
        try:
            from memory_profiler import memory_usage
        except ImportError:
            raise ImportError("memory_profiler module is required for memory profiling.")

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            mem_usage = memory_usage((func, args, kwargs), interval=0.1)
            result = func(*args, **kwargs)
            print(f"Memory usage for {func.__name__}: {max(mem_usage):.2f} MiB")
            return result

        return wrapper
