import argparse
import yaml
import scalpl
import glob
import tqdm
import pandas as pd

from src.time_profiler import time_profiler as tp
from src.data_loader import get_file_list
from src.uproot_processor import UprootProcessor

from src.executors.sequential import SequentialExecutor
from src.executors.futures import FuturesExecutor
from src.executors.dask import DaskLocalExecutor, DaskGatewayExecutor
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
            config = scalpl.Cut(yaml.safe_load(file))
            return config
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Config file not found at path: {file_path}") from e
    except yaml.YAMLError as e:
        raise ValueError(f"YAML error: {e}") from e


class Benchmark:
    def __init__(self, config_path=None):
        self.executor = None
        self.processor = None
        self.report_df = pd.DataFrame()
        self.col_stats = pd.DataFrame()

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
            self.executor.wait_for_workers(0) if reset_workers else None
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

        self.col_stats = self.processor.run_processor(files, self.executor)

    def update_report(self):        
        report = {
            "n_files": self.n_files,
            "n_columns_read": len(self.processor.columns),
            "n_events": self.col_stats.nevents.sum(),
            "loaded_columns": self.config.get('processor.load_columns_into_memory', False),
            "worker_operation_time": self.config.get('processor.worker_operation_time', 0),
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

        # Add custom labels
        for label, value in self.config.get('custom_labels', {}).items():
            if label not in report:
                report[label] = value

        # Add measurements to common DataFrame
        self.report_df = pd.concat([
            self.report_df,
            pd.DataFrame([report])
        ]).reset_index(drop=True)


def run_benchmark(config_path):
    # Read config from YAML or a directory with multiple YAMLs
    if config_path.endswith(".yaml") or config_path.endswith(".yml"):
        configs = [config_path]
    else:
        configs = glob.glob(config_path+"/*.yaml") + glob.glob(config_path+"/*.yml")

    b = Benchmark()
    for config_file in tqdm.tqdm(configs):
        # print(f"> Loading config from {config_file}")
        b.reload_config(config_file)
        b.reset(keep_cluster=True, reset_workers=True)
        b.run()
        b.update_report()

    return b.report_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_path', help="Path to YAML config or directory with YAML configs")
    args = parser.parse_args()
    report = run_benchmark(args.config_path)
    print(report)
