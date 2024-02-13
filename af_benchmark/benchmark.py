import argparse
import yaml
import scalpl
import glob
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
    def __init__(self, config_path=None):
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
        if config_path:
            self.reinitialize(config_path)

    def reinitialize(self, config_path):
        tp.reset()

        self.config = read_yaml(config_path)

        # Select executor backend
        self.backend = self.config.get('executor.backend')
        if self.backend in executors:
            self.executor = executors[self.backend]()
        else:
            raise NotImplementedError(
                f"Invalid backend: {self.backend}. Allowed values are: {executors.keys()}"
            )

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

    def update_report(self):
        # print(tp.report_df)
        run_time = tp.report_df.loc[
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

    def print_report(self):
        print(self.report_df)


def run_benchmark(args):
    
    if args.config_path.endswith(".yaml") or args.config_path.endswith(".yml"):
        configs = [args.config_path]
    else:
        configs = glob.glob(args.config_path+"/*.yaml") + glob.glob(args.config_path+"/*.yml")

    b = Benchmark()
    for config_file in configs:
        print(f"> Loading config from {config_file}")
        b.reinitialize(config_file)
        b.run()
        b.update_report()

    b.print_report()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', help="Path to YAML config or directory with YAML configs")
    args = parser.parse_args()
    run_benchmark(args)
