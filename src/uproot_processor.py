from src.time_profiler import time_profiler as tp
import pandas as pd
import numpy as np
import uproot
import time

class UprootProcessor:
    def __init__(self, config):
        self.config = config

    def open_nanoaod(self, file_path, **kwargs):
        tree = uproot.open(file_path,timeout=300)["Events"]
        return tree

    def get_column_list(self, file):
        column_read_method = self.config.get('processor.columns.method', None)
        if not column_read_method:
            raise ValueError("processor.columns.method must be specified!")

        tree = self.open_nanoaod(file)

        if column_read_method=='collections':
            collections_to_read = self.config.get('processor.columns.values', [])
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

        elif column_read_method=='column_list':
            columns_to_read = self.config.get('processor.columns.values', [])
            # Explicitly specified columns
            missing_columns = [c for c in columns_to_read if c not in tree.keys()]
            if missing_columns:
                raise ValueError(f"Error reading columns: {', '.join(missing_columns)}")
            self.columns = columns_to_read

            # elif isinstance(columns_to_read, int):
        elif column_read_method=='n_columns':
            n_columns_to_read = self.config.get('processor.columns.values', 0)
            # Number of columns to read
            if n_columns_to_read < 0:
                raise ValueError("Number of columns can't be negative.")
            self.columns = list(tree.keys())[:n_columns_to_read]
            if len(self.columns) < n_columns_to_read:
                print(f"Trying to read {n_columns_to_read} columns, but only {len(self.columns)} present in file -> will read all of them.")

        else:
            raise ValueError(f"Invalid value of processor.columns.method parameter: {column_read_method}")


    @tp.enable
    def run_processor(self, files, executor, **kwargs):
        parallelize_over = self.config.get('processor.parallelize_over', 'files')

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

        col_stats = executor.execute(self.worker_func, args, **kwargs)

        return pd.concat(col_stats).reset_index(drop=True)  


    def worker_func(self, args, **kwargs):
        column_stats = []
        col_stats_df = pd.DataFrame()
        files = args["files"]
        columns = args["columns"]
        for file in files:
            tree = self.open_nanoaod(file)
            for column in columns:
                col_stats = self.process_column(tree, column, **kwargs)
                col_stats_df = pd.concat([col_stats_df, col_stats])
        self.run_worker_operation()
        return col_stats_df


    def process_column(self, tree, column, **kwargs):
        column_data = tree[column]
        col_stats = pd.DataFrame([{
            "file": tree.file.file_path,
            "column": column,
            "compressed_bytes": column_data.compressed_bytes,
            "uncompressed_bytes": column_data.uncompressed_bytes,
            "nevents": tree.num_entries
        }])
        if self.config.get('processor.load_columns_into_memory', False):
            self.load_columns_into_memory(column_data)
        return col_stats


    def load_columns_into_memory(self, column_data):
        data_in_memory = np.array([])
        if isinstance(column_data, list):
            for item in column_data:
                data_in_memory = np.concatenate((data_in_memory, item.array()))
        else:
            data_in_memory = column_data.array()


    def run_worker_operation(self):
        timeout = self.config.get('processor.worker_operation_time', 0)
        if timeout==0:
            return

        # compute pi until timeout
        start_time = time.time()
        pi = k = 0
        while True:
            pi += (4.0 * (-1)**k) / (2*k + 1)
            k += 1
            if time.time() - start_time > timeout:
                return
