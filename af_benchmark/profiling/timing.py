import cProfile, pstats
import functools
import pandas as pd


class TimeProfiler:
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.report_df = pd.DataFrame()
    def profile(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.profiler.enable()
            result = func(*args, **kwargs)
            self.profiler.disable()
            stats = pstats.Stats(self.profiler)

            # find line with highest total time
            # max_stats = max(stats.stats.items(), key=lambda item: item[1][2])
            # file_line_func, function_metrics = max_stats
            # file_path, line_number, function_name = file_line_func
            # total_calls, primitive_calls, total_time, time_per_call, _ = function_metrics
            total_time = sum(v[2] for v in stats.stats.values())


            df = pd.DataFrame([{
                'function': func.__name__,
                'tottime': total_time,
            }])
            
            self.report_df = pd.concat([self.report_df, df])
            return result
        return wrapper

    def print_stats(self):
        print(self.report_df)
