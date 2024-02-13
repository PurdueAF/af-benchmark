import cProfile, pstats
import functools
import pandas as pd
import inspect

class TimeProfiler:
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.report_df = pd.DataFrame()
        self.enabled = []

    def reset(self):
        self.__init__()

    def enable(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            func_name = func.__name__
            func_file = inspect.getfile(func)
            func_ln = inspect.getsourcelines(func)[1]
            func_desc = (func_name, func_file, func_ln)
            if func_desc not in self.enabled:
                self.enabled.append(func_desc)
            return result
        return wrapper
            
            
    def profile(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.profiler.enable()
            result = func(*args, **kwargs)
            self.profiler.disable()

            stats = pstats.Stats(self.profiler)

            for func_desc in self.enabled:
                func_name, func_file, func_ln = func_desc

                # Find cumulative time value corresponding to this function
                func_time = [
                    v[3] for k, v in stats.stats.items()
                    if (
                        (k[0] == func_file) and
                        (k[1] == func_ln) and
                        (k[2] == func_name)
                    )
                ]

                # remove global path
                # TODO: make this relative to base dir of the package
                func_file_short = "/".join(func_file.split("/")[-3:])
    
                # If by accident there are multiple results - save all
                for ft in func_time:
                    df = pd.DataFrame([{
                        'func_file': func_file_short,
                        'func_ln': func_ln,
                        'func_name': func_name,
                        'func_time': ft,
                    }])
                    self.report_df = pd.concat([self.report_df, df]).reset_index(drop=True)
            return result
        return wrapper

    def print_stats(self):
        print(self.report_df)


time_profiler = TimeProfiler()