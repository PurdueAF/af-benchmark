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
        self.col_stats=pd.DataFrame()

    @tp.enable
    @abstractmethod
    def open_nanoaod(self, file_path, **kwargs):
        return

    @tp.enable
    @abstractmethod
    def read_columns(self, tree, **kwargs):
        return

    @tp.enable
    @abstractmethod
    def run_operation(self, columns, **kwargs):
        return


class UprootProcessor(BaseProcessor):
    def __init__(self, config):
        self.config = config
        self.col_stats = pd.DataFrame(
            columns = [
                "file",
                "column",
                "compressed_bytes",
                "uncompressed_bytes"
            ]
        )

    @tp.enable
    def open_nanoaod(self, file_path, **kwargs):
        tree = uproot.open(file_path)["Events"]
        return tree

    @tp.enable
    def read_columns(self, tree, **kwargs):
        columns_to_read = self.config.get('processor.columns')
        column_data = {}
        for column in columns_to_read:
            if column in tree.keys():
                branch = tree[column]
            else:
                raise ValueError(f"Error reading column: {column}")
            column_data[column] = branch
            col_stats = pd.DataFrame([{
                "file": tree.file.file_path,
                "column": column,
                "compressed_bytes": branch.compressed_bytes,
                "uncompressed_bytes": branch.uncompressed_bytes
            }])
            self.col_stats = pd.concat([self.col_stats, col_stats]).reset_index(drop=True)
        return column_data

    @tp.enable
    def run_operation(self, columns, **kwargs):
        operation = self.config.get('processor.operation')
        results = {}
        for name, data in columns.items():
            if operation == 'array':
                # just load it in memory
                data.array()
            if operation == 'mean':        
                results[name] = np.mean(data.array())
        return results



class NanoEventsProcessor(BaseProcessor):
    def open_nanoaod(self, file_path, **kwargs):
        tree = NanoEventsFactory.from_root(
            file_path,
            schemaclass=NanoAODSchema.v6,
            uproot_options={"timeout": 120}
        ).events()
        return tree

    def read_columns(self, tree, **kwargs):
        columns_to_read = self.config.get('processor.columns')
        column_data = {}        
        for column in columns_to_read:
            if column in tree.fields:
                column_data[column] = tree[column]
            elif "_" in column:
                branch, leaf = column.split("_")
                column_data[column] = tree[branch][leaf]
            else:
                raise ValueError(f"Error reading column: {column}")
        return column_data

    def run_operation(self, columns, **kwargs):
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

        