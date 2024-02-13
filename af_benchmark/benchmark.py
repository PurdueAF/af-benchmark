import argparse
import yaml
import scalpl
import pandas as pd

from profiling.timing import time_profiler as tp
from data_access.loader import get_file_list
from processing.handler import handlers

from executor.sequential import SequentialExecutor
from executor.futures import FuturesExecutor
from executor.dask import DaskLocalExecutor, DaskGatewayExecutor
executors = {
    'sequential': SequentialExecutor,
    'futures': FuturesExecutor,
    'dask-local': DaskLocalExecutor,
    'dask-gateway': DaskGatewayExecutor
}


@tp.enable
def read_yaml(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            # this allows dotted notation while parsing config
            config = scalpl.Cut(config)
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at path: {file_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"YAML error: {e}")


class Benchmark:
    @tp.enable
    def __init__(self, config_path):
        print("  > Reading config")
        self.config = read_yaml(config_path)

        self.report_df = pd.DataFrame(
            columns=[
                "dataset",
                "n_files",
                "n_columns_read",
                "n_workers",
                "total_time",
                "operation",
                "executor",
                "col_handler",
            ]
        )
        self.n_files = 0

        print("  > Initializing executor")
        # Select executor backend
        self.backend = self.config.get('executor.backend')
        if self.backend in executors:
            self.executor = executors[self.backend]()
        else:
            raise NotImplementedError(
                f"Invalid backend: {self.backend}. Allowed values are: {executors.keys()}"
            )

        print("  > Initializing column handler")
        # Select file handler method
        self.method = self.config.get('processing.method')
        if self.method in handlers:
            self.handler = handlers[self.method](self.config)
        else:
            raise NotImplementedError(
                f"Invalid method: {self.method}. Allowed values are: {handlers.keys()}"
            )


    @tp.profile
    @tp.enable
    def run(self):
        files = get_file_list(self)
        self.n_files = len(files)

        trees = self.executor.execute(
            self.handler.open_nanoaod, files
        )

        columns_by_file = self.executor.execute(
            self.handler.read_columns, trees
        )
        
        outputs = self.executor.execute(
            self.handler.run_operation, columns_by_file
        )

        return outputs

    def report(self):
        run_stats = tp.report_df.loc[
            tp.report_df.func_name=="run",
            "func_time"
        ].values[0]

        self.report_df = pd.concat([
            self.report_df,
            pd.DataFrame([{
                "dataset": "",
                "n_files": self.n_files,
                "n_columns_read": len(self.config.get('processing.columns')),
                "n_workers": self.executor.get_n_workers(),
                "total_time": run_time,
                "operation": self.config.get('processing.operation'),
                "executor": self.backend,
                "col_handler": self.method,
            }])
        ])

        print(self.report_df)


def run_benchmark(args):
    print("> Creating benchmark ...")
    b = Benchmark(args.config_file)
    print("> Starting benchmark ...")
    outputs = b.run()
    print("> Benchmark finished")
    b.report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help="Path to YAML config")
    args = parser.parse_args()
    run_benchmark(args)
