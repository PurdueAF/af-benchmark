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
    def get_column_names(self, tree):
        return

    @tp.enable
    def read_columns(self, trees, executor, parallelize_over):
        column_names = self.get_column_names(trees[0])
        if not parallelize_over:
            parallelize_over = "files"
        if parallelize_over=="files":
            column_data = executor.execute(
                self.read_by_file,
                trees,
                column_names=column_names
            )
        elif parallelize_over=="columns":
            column_data = executor.execute(
                self.read_by_column,
                column_names,
                trees=trees
            )
        else:
            raise ValueError(f"Can't parallelize over {parallelize_over}")

        return column_data     

    def read_by_file(self, tree, **kwargs):
        column_names = kwargs.get("column_names", [])
        column_data = {}
        column_stats = []
        for column_name in column_names:
            result = self.read_column(tree, column_name)
            column_data[column_name] = result["data"]
            if "stats" in result.keys():
                column_stats.append(result["stats"])
        if column_stats:
            col_stats_df = pd.concat(column_stats)
        else:
            col_stats_df = pd.DataFrame()
        return column_data, col_stats_df

    def read_by_column(self, column_name, **kwargs):
        trees = kwargs.get("trees", [])
        column_data = []
        column_stats = []
        for tree in trees:
            result = self.read_column(tree, column_name)
            column_data.append(result["data"])
            if "stats" in result.keys():
                column_stats.append(result["stats"])
        if column_stats:
            col_stats_df = pd.concat(column_stats)
        else:
            col_stats_df = pd.DataFrame()
        return {column_name: column_data}, col_stats_df

    @abstractmethod
    def read_column(self, tree, column):
        return

    @tp.enable
    def run_operation(self, columns, executor, **kwargs):        
        return executor.execute(self.execute_func, columns, **kwargs)

    def execute_func(self, columns, **kwargs):
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

    def get_column_names(self, tree):
        columns_to_read = self.config.get('processor.columns')
        if isinstance(columns_to_read, list):
            if any(c not in tree.keys() for c in columns_to_read):
                raise ValueError(f"Error reading column: {column}")
            column_names = columns_to_read
        elif isinstance(columns_to_read, int):
            column_names = list(tree.keys())[:columns_to_read]
            if len(column_names)<columns_to_read:
                raise ValueError(f"Trying to read {columns_to_read} columns, but only {len(column_names)} present in file.")
        else:
            raise ValueError(f"Incorrect type of processor.columns parameter: {type(columns_to_read)}")
        return column_names

    def read_column(self, tree, column_name):
        column_data = tree[column_name]
        col_stats = pd.DataFrame([{
            "file": tree.file.file_path,
            "column": column_name,
            "compressed_bytes": column_data.compressed_bytes,
            "uncompressed_bytes": column_data.uncompressed_bytes
        }])
        return {"data": column_data, "stats": col_stats}

    def run_operation_(self, columns, **kwargs):
        operation = self.config.get('processor.operation', None)
        if not operation:
            return
        results = {}
        for name, data in columns.items():
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
                results[name] = np.mean(data_in_memory)
            elif operation == 'sum':        
                results[name] = np.sum(data_in_memory)
        return results



class NanoEventsProcessor(BaseProcessor):

    def open_nanoaod_(self, file_path, **kwargs):
        tree = NanoEventsFactory.from_root(
            file_path,
            schemaclass=NanoAODSchema.v6,
            uproot_options={"timeout": 120}
        ).events()
        return tree

    def get_column_names(self, tree):
        column_names = self.config.get('processor.columns')
        if not isinstance(column_names, list):
            raise NotImplementedError("For NanoEventsProcessor, only explicit list of columns is currently possible")
        return column_names

    def read_column(self, tree, column_name):
        if column_name in tree.fields:
            column_data = tree[column_name]
        elif "_" in column_name:
            branch, leaf = column_name.split("_")
            column_data = tree[branch][leaf]
        else:
            raise ValueError(f"Error reading column: {column_name}")
        return {"data": column_data}

    def run_operation_(self, columns, **kwargs):
        operation = self.config.get('processor.operation')
        results = {}
        for name, data in columns.items():
            if operation == 'mean':        
                results[name] = np.mean(data)
        return results


processors = {
    'uproot': UprootProcessor,
    'nanoevents': NanoEventsProcessor
}

        