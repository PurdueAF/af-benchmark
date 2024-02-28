from profiling.timing import time_profiler as tp
import pandas as pd
import numpy as np
import uproot

class UprootProcessor:
    def __init__(self, config):
        self.config = config

    def open_nanoaod(self, file_path, **kwargs):
        tree = uproot.open(file_path)["Events"]
        return tree

    def get_column_list(self, file):
        columns_to_read = self.config.get('processor.columns')
        tree = self.open_nanoaod(file)
        if isinstance(columns_to_read, list):
            if any(c not in tree.keys() for c in columns_to_read):
                raise ValueError(f"Error reading column: {column}")
            self.columns = columns_to_read
        elif isinstance(columns_to_read, int):
            self.columns = list(tree.keys())[:columns_to_read]
            if len(self.columns)<columns_to_read:
                raise ValueError(f"Trying to read {columns_to_read} columns, but only {len(self.columns)} present in file.")
        else:
            raise ValueError(f"Incorrect type of processor.columns parameter: {type(columns_to_read)}")

    @tp.enable
    def read_columns(self, files, executor, parallelize_over):
        arg_dict = {
            "files": files,
            "columns": self.columns
        }
        if parallelize_over == "files":
            args = [{"files": [file], "columns": self.columns} for file in files]
        elif parallelize_over == "columns":
            args = [{"files": files, "columns": [col]} for col in self.columns]
        else:
            args = [{"files": [file], "columns": [col]} for file in files for col in self.columns]

        column_data = executor.execute(self.read_columns_func, args)

        return column_data     

    def read_columns_func(self, args):
        column_data = []
        column_stats = []
        files = args["files"]
        columns = args["columns"]
        for file in files:
            file_column_data = {}
            for column in columns:
                result = self.read_column(file, column)
                file_column_data[column] = result["data"]
                if "stats" in result.keys():
                    column_stats.append(result["stats"])
            column_data.append(file_column_data)
        col_stats_df = pd.DataFrame()
        if column_stats:
            col_stats_df = pd.concat(column_stats)
        return column_data, col_stats_df
    
    def read_column(self, file, column):
        tree = self.open_nanoaod(file)
        column_data = tree[column]
        col_stats = pd.DataFrame([{
            "file": tree.file.file_path,
            "column": column,
            "compressed_bytes": column_data.compressed_bytes,
            "uncompressed_bytes": column_data.uncompressed_bytes
        }])
        return {"data": column_data, "stats": col_stats}

    @tp.enable
    def run_operation(self, columns, executor, **kwargs):        
        return executor.execute(self.run_operation_func, columns, **kwargs)

    def run_operation_func(self, columns, **kwargs):
        if isinstance(columns, tuple):
            result = self.run_operation_(columns[0], **kwargs)
            col_stats = columns[1]
        else:
            result = self.run_operation(columns, **kwargs)
            col_stats = None
        return result, col_stats

    def run_operation_(self, column_data, **kwargs):
        operation = self.config.get('processor.operation', None)
        if not operation:
            return
        results = {}
        for file_column_data in column_data:
            for data in file_column_data.values():
                data_in_memory = np.array([])
                if isinstance(data, list):
                    for item in data:
                        data_in_memory = np.concatenate((data_in_memory, item.array()))
                else:
                    data_in_memory = data.array()
    
                if operation == 'array':
                    # just load it in memory
                    continue
                elif operation == 'mean':        
                    np.mean(data_in_memory)
                elif operation == 'sum':        
                    np.sum(data_in_memory)
        return results

        