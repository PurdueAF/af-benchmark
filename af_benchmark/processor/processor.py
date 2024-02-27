from abc import ABC, abstractmethod
from profiling.timing import time_profiler as tp
import pandas as pd
import numpy as np
import uproot
from coffea.nanoevents import NanoEventsFactory, NanoAODSchema


class BaseProcessor(ABC):
    """A base processor class
    """
    def __init__(self, config):
        self.config=config

    @tp.enable
    def open_nanoaod(self, files, executor, **kwargs):
        return executor.execute(self.open_nanoaod_, files, **kwargs)

    @abstractmethod
    def open_nanoaod_(self, file_path, **kwargs):
        return

    @abstractmethod
    def get_column_list(self, file):
        return

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

    @abstractmethod
    def read_column(self, file, column):
        return

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

    @abstractmethod
    def run_operation_(self, columns, **kwargs):
        return


class UprootProcessor(BaseProcessor):
    def __init__(self, config):
        self.config = config

    def open_nanoaod_(self, file_path, **kwargs):
        tree = uproot.open(file_path)["Events"]
        return tree

    def get_column_list(self, file):
        columns_to_read = self.config.get('processor.columns')
        tree = self.open_nanoaod_(file)
        if isinstance(columns_to_read, list):
            if any(c not in tree.keys() for c in columns_to_read):
                raise ValueError(f"Error reading column: {column}")
            self.column = columns_to_read
        elif isinstance(columns_to_read, int):
            self.columns = list(tree.keys())[:columns_to_read]
            if len(self.columns)<columns_to_read:
                raise ValueError(f"Trying to read {columns_to_read} columns, but only {len(self.columns)} present in file.")
        else:
            raise ValueError(f"Incorrect type of processor.columns parameter: {type(columns_to_read)}")

    def read_column(self, file, column):
        tree = self.open_nanoaod_(file)
        column_data = tree[column]
        col_stats = pd.DataFrame([{
            "file": tree.file.file_path,
            "column": column,
            "compressed_bytes": column_data.compressed_bytes,
            "uncompressed_bytes": column_data.uncompressed_bytes
        }])
        return {"data": column_data, "stats": col_stats}

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



class NanoEventsProcessor(BaseProcessor):

    def open_nanoaod_(self, file_path, **kwargs):
        tree = NanoEventsFactory.from_root(
            file_path,
            schemaclass=NanoAODSchema.v6,
            uproot_options={"timeout": 120}
        ).events()
        return tree

    def get_column_list(self, file):
        self.columns = self.config.get('processor.columns')
        tree = self.open_nanoaod_(file)
        if not isinstance(self.columns, list):
            raise NotImplementedError("For NanoEventsProcessor, only explicit list of columns is currently possible")

    def read_column(self, file, column):
        tree = self.open_nanoaod_(file)
        if column in tree.fields:
            column_data = tree[column]
        elif "_" in column:
            branch, leaf = column.split("_")
            column_data = tree[branch][leaf]
        else:
            raise ValueError(f"Error reading column: {column}")
        return {"data": column_data}

    def run_operation_(self, column_data, **kwargs):
        operation = self.config.get('processor.operation')
        results = {}
        for file_column_data in column_data:
            for data in column_data.values():
                if operation == 'mean':        
                    np.mean(data)
        return results


processors = {
    'uproot': UprootProcessor,
    'nanoevents': NanoEventsProcessor
}

        