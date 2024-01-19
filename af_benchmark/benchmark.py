import argparse
import yaml
import scalpl
from data_access.loader import get_file_list
from processing.tools import open_nanoaod, validate_columns, run_operation
from engine.executor import executors
from profiling.timing import TimeProfiler
tp = TimeProfiler()

@tp.profile
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
    def __init__(self, config_path):
        self.config = read_yaml(config_path)
        backend = self.config.get('engine.backend')
        if backend in executors:
            self.executor = executors[backend]()
        else:
            raise NotImplementedError(
                f"Invalid backend: {backend}. Allowed values are: {executors.keys()}"
            )

    @tp.profile
    def run(self):
        files = get_file_list(self)

        trees = self.executor.execute(
            open_nanoaod, files, method=self.config.get('processing.method')
        )

        columns_by_file = self.executor.execute(
            validate_columns, trees, columns_to_read=self.config.get('processing.columns')
        )

        outputs = self.executor.execute(
            run_operation, columns_by_file, operation=self.config.get('processing.operation')
        )

        return outputs


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help="Path to YAML config")
    args = parser.parse_args()

    b = Benchmark(args.config_file)
    outputs = b.run()
    print(outputs)
    tp.print_stats()
