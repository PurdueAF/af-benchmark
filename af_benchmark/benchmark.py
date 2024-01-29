import argparse
import yaml
import scalpl

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
        self.config = read_yaml(config_path)

        # Select executor backend
        backend = self.config.get('executor.backend')
        if backend in executors:
            self.executor = executors[backend]()
        else:
            raise NotImplementedError(
                f"Invalid backend: {backend}. Allowed values are: {executors.keys()}"
            )

        # Select file handler method
        method = self.config.get('processing.method')
        if method in handlers:
            self.handler = handlers[method](self.config)
        else:
            raise NotImplementedError(
                f"Invalid method: {handler}. Allowed values are: {handlers.keys()}"
            )


    @tp.enable
    def run(self):
        files = get_file_list(self)

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


@tp.profile
def run_benchmark(args):
    b = Benchmark(args.config_file)
    outputs = b.run()
    print(outputs)
    print(b.handler.col_stats)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help="Path to YAML config")
    args = parser.parse_args()
    run_benchmark(args)
    tp.print_stats()
