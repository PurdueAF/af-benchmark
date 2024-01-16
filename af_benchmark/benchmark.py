import argparse
import yaml
from processing.tools import open_nanoaod, validate_columns, run_operation
from parallelization.base import maybe_parallelize

class Benchmark:
    def __init__(self, yaml_file_path):
        self.parameters = self._read_yaml(yaml_file_path)
        parallel_params = self.parameters.get('parallelization', {})
        do_parallelize = parallel_params.get('parallelize', False)
        backend = parallel_params.get('backend', None)
        if do_parallelize:
            if backend=='dask-local':
                from dask.distributed import LocalCluster, Client
                self.dask_cluster = LocalCluster()
                self.dask_client = Client(self.dask_cluster)
                print("Created Dask LocalCluster()")

    def __del__(self):
        if hasattr(self, 'dask_cluster') and self.dask_cluster is not None:
            self.dask_cluster.close()
            print("Closed Dask cluster")
        if hasattr(self, 'dask_client') and self.dask_client is not None:
            self.dask_client.close()

    def _read_yaml(self, yaml_file_path):
        try:
            with open(yaml_file_path, 'r') as file:
                yaml_data = yaml.safe_load(file)
                return yaml_data
        except FileNotFoundError:
            raise FileNotFoundError(f"YAML file not found at path: {yaml_file_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")

    def _maybe_parallelize(self, func, args, **kwargs):
        is_list = isinstance(args, list)
        is_dict = isinstance(args, dict)
        if not (is_list or is_dict):
            raise ValueError("Unsupported 'args' type. Use either list or dict.")

        if is_dict:
            keys, args = zip(*args.items())

        results = maybe_parallelize(self, func, args, kwargs)            

        if is_dict:
            results = dict(zip(keys, results))

        return results
    
    def run(self):
        files = self.parameters.get('data-access', {}).get('files', [])
        processing_method = self.parameters.get('processing', {}).get('method', 'nanoevents')
        trees = self._maybe_parallelize(open_nanoaod, files, method=processing_method)

        columns_to_read = self.parameters.get('processing', {}).get('columns', [])
        columns_by_file = self._maybe_parallelize(validate_columns, trees, columns_to_read=columns_to_read)

        operation = self.parameters.get('processing', {}).get('operation', None)
        outputs = self._maybe_parallelize(run_operation, columns_by_file, operation=operation)
        print(outputs)   


def parse_command_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('yaml_file', help="Path to YAML config")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_command_line_args()
    yaml_file_path = args.yaml_file

    b = Benchmark(yaml_file_path)
    b.run()
