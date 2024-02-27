import argparse
import yaml
import scalpl
import glob
import pandas as pd

from profiling.timing import time_profiler as tp
from data_access.loader import get_file_list
from processor.processor import processors

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
        self.report_df = pd.DataFrame()
        if config_path:
            self.reload_config(config_path)

    def reload_config(self, config_path):
        self.config = read_yaml(config_path)
        self.reset()

    def reset(self):
        self.reset_profiler()
        self.reset_executor()
        self.reset_processor()        

    def reset_profiler(self):
        tp.reset()

    def reset_executor(self):
        # Select executor backend
        self.backend = self.config.get('executor.backend')
        if self.backend in executors:
            self.executor = executors[self.backend]()
        else:
            raise NotImplementedError(
                f"Invalid backend: {self.backend}. Allowed values are: {executors.keys()}"
            )

    def reset_processor(self):
        # Select processor method
        self.method = self.config.get('processor.method')
        if self.method in processors:
            self.processor = processors[self.method](self.config)
        else:
            raise NotImplementedError(
                f"Invalid method: {self.method}. Allowed values are: {processors.keys()}"
            )

    @tp.profile
    @tp.enable
    def run(self):
        files = get_file_list(self)
        trees = self.executor.execute(self.processor.open_nanoaod, files)

        column_data = self.processor.read_columns(
            trees,
            self.executor,
            parallelize_over=self.config.get('processor.parallelize_over')
        )

        outputs_ = self.executor.execute(self.processor.run_operation, column_data)

        self.col_stats = pd.concat([o[1] for o in outputs_]).reset_index(drop=True)
        
        outputs = [o[0] for o in outputs_]

        return outputs

    def update_report(self):
        n_cols_read = self.config.get('processor.columns')
        if isinstance(n_cols_read, list):
            n_cols_read = len(n_cols_read)
        
        report = {
            "n_files": self.n_files,
            "n_columns_read": n_cols_read,
            "processor": self.method,
            "operation": self.config.get('processor.operation'),
            "executor": self.backend,
            "n_workers": self.executor.get_n_workers(),
        }

        # Add column size measurements
        col_stats = self.col_stats
        if "compressed_bytes" in col_stats.columns:
            report.update({
                "compressed_bytes": col_stats.compressed_bytes.sum()
            })
        if "uncompressed_bytes" in col_stats.columns:
            report.update({
                "uncompressed_bytes": col_stats.uncompressed_bytes.sum()
            })

        # Add timing measurements
        report.update(
            dict(
                zip(tp.report_df.func_name, tp.report_df.func_time)
            )
        )

        self.report_df = pd.concat([
            self.report_df,
            pd.DataFrame([report])
        ]).reset_index(drop=True)


def run_benchmark(config_path):
    
    if config_path.endswith(".yaml") or config_path.endswith(".yml"):
        configs = [config_path]
    else:
        configs = glob.glob(config_path+"/*.yaml") + glob.glob(config_path+"/*.yml")

    b = Benchmark()
    for config_file in configs:
        print(f"> Loading config from {config_file}")
        b.reload_config(config_file)
        b.reset()
        b.run()
        b.update_report()

    return b.report_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', help="Path to YAML config or directory with YAML configs")
    args = parser.parse_args()
    report = run_benchmark(args.config_path)
    print(report)
