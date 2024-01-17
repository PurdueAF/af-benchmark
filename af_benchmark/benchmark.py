import argparse
import yaml
from processing.tools import open_nanoaod, validate_columns, run_operation
from engine.executor import executors


def read_yaml(file_path):
    try:
        with open(file_path, 'r') as file:
            config = yaml.safe_load(file)
            return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Config file not found at path: {file_path}")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")


class Benchmark:
    def __init__(self, config_path):
        self.parameters = read_yaml(config_path)
        backend = self.parameters.get('engine', {}).get('backend', None)
        if backend in executors:
            self.executor = executors[backend]()
        else:
            raise NotImplementedError(f"Invalid backend: {backend}. Allowed values are: {executors.keys()}")

    def __del__(self):
        del self.executor
    
    def run(self):
        files = self.parameters.get('data-access', {}).get('files', [])
        processing_method = self.parameters.get('processing', {}).get('method', 'nanoevents')
        trees = self.executor.execute(open_nanoaod, files, method=processing_method)

        columns_to_read = self.parameters.get('processing', {}).get('columns', [])
        columns_by_file = self.executor.execute(validate_columns, trees, columns_to_read=columns_to_read)

        operation = self.parameters.get('processing', {}).get('operation', None)
        outputs = self.executor.execute(run_operation, columns_by_file, operation=operation)
        print(outputs)   


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help="Path to YAML config")
    args = parser.parse_args()

    b = Benchmark(args.config_file)
    b.run()
