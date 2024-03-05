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
        columns_to_read = self.config.get('processor.columns', [])
        collections_to_read = self.config.get('processor.collections', [])

        if columns_to_read and collections_to_read:
            raise ValueError("Specifying columns and collections at the same time is not allowed!")

        tree = self.open_nanoaod(file)

        if collections_to_read:
            # All columns from given collections
            missing_collections = [
                collection for collection in collections_to_read
                    if not any(column.startswith(collection) for column in tree.keys())
            ]
            if missing_collections:
                raise ValueError(f"Missing collections: {', '.join(missing_collections)}")
            self.columns = [
                column for column in tree.keys()
                    if any(column.startswith(collection) for collection in collections_to_read)
            ]

        elif isinstance(columns_to_read, list):
            # Explicitly specified columns
            missing_columns = [c for c in columns_to_read if c not in tree.keys()]
            if missing_columns:
                raise ValueError(f"Error reading columns: {', '.join(missing_columns)}")
            self.columns = columns_to_read

        elif isinstance(columns_to_read, int):
            # Number of columns to read
            if columns_to_read < 0:
                raise ValueError("Number of columns can't be negative.")
            self.columns = list(tree.keys())[:columns_to_read]
            if len(self.columns) < columns_to_read:
                raise ValueError(f"Trying to read {columns_to_read} columns, but only {len(self.columns)} present in file.")

        else:
            raise ValueError(f"Incorrect type of processor.columns parameter: {type(columns_to_read)}")


    @tp.enable
    def process_columns(self, files, executor, **kwargs):
        parallelize_over = kwargs.get("parallelize_over", 'files')
        arg_dict = {
            "files": files,
            "columns": self.columns
        }
        if parallelize_over == "files":
            args = [{"files": [file], "columns": self.columns} for file in files]
        elif parallelize_over == "columns":
            args = [{"files": files, "columns": [col]} for col in self.columns]
        elif parallelize_over == "files_and_columns":
            args = [{"files": [file], "columns": [col]} for file in files for col in self.columns]
        else:
            raise ValueError(f"Incorrect parameter: parallelize_over={parallelize_over}")

        col_stats = executor.execute(self.process_columns_func, args, **kwargs)

        return pd.concat(col_stats).reset_index(drop=True)  


    def process_columns_func(self, args, **kwargs):
        column_stats = []
        col_stats_df = pd.DataFrame()
        files = args["files"]
        columns = args["columns"]
        for file in files:
            for column in columns:
                col_stats = self.process_column(file, column, **kwargs)
                col_stats_df = pd.concat([col_stats_df, col_stats])
        return col_stats_df


    def process_column(self, file, column, **kwargs):
        tree = self.open_nanoaod(file)
        column_data = tree[column]
        col_stats = pd.DataFrame([{
            "file": tree.file.file_path,
            "column": column,
            "compressed_bytes": column_data.compressed_bytes,
            "uncompressed_bytes": column_data.uncompressed_bytes,
            "nevents": tree.num_entries
        }])
        self.run_operation(column_data)
        return col_stats


    def run_operation(self, column_data, **kwargs):
        operation = self.config.get('processor.operation', 'nothing')

        if (not operation) or (operation=='nothing'):
            return

        data_in_memory = np.array([])
        if isinstance(column_data, list):
            for item in column_data:
                data_in_memory = np.concatenate((data_in_memory, item.array()))
        else:
            data_in_memory = column_data.array()

        if operation == 'load_into_memory':
            return
        elif operation == 'mean':        
            np.mean(data_in_memory)
        elif operation == 'sum':        
            np.sum(data_in_memory)


        