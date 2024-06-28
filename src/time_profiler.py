import cProfile, pstats
import functools
import pandas as pd
import inspect


class TimeProfiler:
    def __init__(self):
        self.profiler = cProfile.Profile()
        self.report_df = pd.DataFrame()
        self.enabled = []
        self.full_outputs = []

    def reset(self):
        self.__init__()

    def enable(self, func):
        '''
        @tp.enable decorator is used to indicate that we want to extract
        the cumulative timing measurement for the function to which it is applied.
        '''
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            result = func(*args, **kwargs)
            func_name = func.__name__
            func_file = inspect.getfile(func)
            func_ln = inspect.getsourcelines(func)[1]
            func_desc = (func_file, func_ln, func_name)
            if func_desc not in self.enabled:
                self.enabled.append(func_desc)
            return result
        return wrapper
            
            
    def profile(self, func):
        '''
        @tp.profile decorator is applied to a function for which we want to run cProfile
        (ideally it should be the outermost function in the workflow).
        Rather than displaying all cProfile outputs, we find rows corresponding to the functions
        selected by @tp.enable, plus some extra rows interesting for us. 
        The outputs are saved in a Pandas DataFrame.
        '''
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            self.profiler.enable()
            result = func(*args, **kwargs)
            self.profiler.disable()

            # Stats are saved in the following format for each function call:
            # (file, line_num, func_name): (ncalls, pcalls, tottime, cumtime, parent_func)
            stats = pstats.Stats(self.profiler)
            self.full_outputs.append(stats)

            aliases = {}

            # In addition to functions included by @tp.enable, we want to measure timing of
            # some low-level functions such as decompression.
            for k,v in stats.stats.items():
                # Actual function name is long => will use alias for nicer looking output
                extra_funcs = {
                    "decompress": "<method 'decompress' of '_lzma.LZMADecompressor' objects>"
                }
                for alias, extra_func in extra_funcs.items():
                    if k[2] == extra_func:
                        self.enabled.append(k)
                        aliases[extra_func] = alias


            for func_desc in self.enabled:

                # Find cumulative time value corresponding to this function
                func_time = [
                    v[3] for k, v in stats.stats.items() if (k == func_desc)
                ]

                func_file, func_ln, func_name = func_desc

                # remove global path
                # TODO: make this relative to base dir of the package
                func_file_short = "/".join(func_file.split("/")[-3:])

                # Edit name to display in output dataframe
                if func_name in aliases:
                    func_name = aliases[func_name]
                func_name = "time:"+func_name

                # If by accident there are multiple results - save all
                self.report_df = pd.concat([
                    self.report_df,
                    pd.DataFrame([
                        {
                            'func_file': func_file_short,
                            'func_ln': func_ln,
                            'func_name': func_name,
                            'func_time': ft
                        } for ft in func_time
                    ])
                ]).reset_index(drop=True)

            return result
        return wrapper

    def print_stats(self):
        print(self.report_df)


time_profiler = TimeProfiler()