import argparse
import yaml
import scalpl
import glob
import pandas as pd

from profiling.timing import time_profiler as tp
from data_access.loader import get_file_list
from processor.uproot_processor import UprootProcessor

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
        self.executor = None
        self.processor = None
        self.report_df = pd.DataFrame()
        self.col_stats = pd.DataFrame()
        self.label = None # arbitrary label
        if config_path:
            self.reload_config(config_path)

    def reload_config(self, config_path):
        self.config = read_yaml(config_path)
        self.reset()

    def reset(self, **kwargs):
        self.reset_profiler(**kwargs)
        self.reset_executor(**kwargs)
        self.reset_processor(**kwargs)

    def reset_profiler(self, **kwargs):
        tp.reset()

    def reset_executor(self, **kwargs):
        keep_cluster = kwargs.get("keep_cluster", False)
        reset_workers = kwargs.get("reset_workers", True)

        self.backend = self.config.get('executor.backend', 'sequential')
        if self.backend not in executors:
            raise NotImplementedError(
                f"Invalid backend: {self.backend}. Allowed values are: {executors.keys()}"
            )

        n_workers = self.config.get('executor.n_workers', 1)

        if keep_cluster and hasattr(self.executor, "cluster"):
            if reset_workers:
                self.executor.wait_for_workers(0)
            self.executor.wait_for_workers(n_workers)
        else:
            self.executor = executors[self.backend](n_workers=n_workers)


    def reset_processor(self, **kwargs):
        self.processor = UprootProcessor(self.config)

    @tp.profile
    @tp.enable
    def run(self):
        files = get_file_list(self)
        self.processor.get_column_list(files[0])

        self.col_stats = self.processor.process_columns(
            files,
            self.executor,
            parallelize_over=self.config.get('processor.parallelize_over', 'files'),
            load_into_memory=True
        )


    def update_report(self):
        n_cols_read = self.config.get('processor.columns', [])
        if isinstance(n_cols_read, list):
            n_cols_read = len(n_cols_read)
        
        report = {
            "label": self.label,
            "n_files": self.n_files,
            "n_columns_read": n_cols_read,
            "n_events": self.col_stats.nevents.sum(),
            "operation": self.config.get('processor.operation', 'nothing'),
            "executor": self.backend,
            "n_workers": self.executor.get_n_workers(),
            "compressed_bytes": self.col_stats.compressed_bytes.sum(),
            "uncompressed_bytes": self.col_stats.uncompressed_bytes.sum(),
        }

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
