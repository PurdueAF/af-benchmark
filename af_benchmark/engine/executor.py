from abc import ABC, abstractmethod


class BaseExecutor(ABC):
    def execute(self, func, args, **kwargs):
        is_list = isinstance(args, list)
        is_dict = isinstance(args, dict)
        if not (is_list or is_dict):
            raise ValueError("Unsupported 'args' type. Use either list or dict.")

        if is_dict:
            keys, args = zip(*args.items())

        results = self._execute(func, args, kwargs) 

        if is_dict:
            results = dict(zip(keys, results))

        return results

    @abstractmethod
    def _execute(self, func, args, kwargs):
        pass


class SequentialExecutor(BaseExecutor):
    def _execute(self, func, args, kwargs):
        return [func(arg, kwargs) for arg in args]

class DaskLocalExecutor(BaseExecutor):
    def __init__(self):
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

    def _execute(self, func, args, kwargs):
        args_sc = self.dask_client.scatter(args)
        dask_futures = [self.dask_client.submit(func, arg, kwargs) for arg in args_sc]
        results = self.dask_client.gather(dask_futures)
        results = list(results)
        return results

class FuturesExecutor(BaseExecutor):
    def _execute(self, func, args, kwargs):
        from concurrent import futures
        with futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(func, args, [kwargs] * len(args)))
        return results


executors = {
    'sequential': SequentialExecutor,
    'dask-local': DaskLocalExecutor,
    'futures': FuturesExecutor,
}



