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

class FuturesExecutor(BaseExecutor):
    def _execute(self, func, args, kwargs):
        from concurrent import futures
        with futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(func, args, [kwargs] * len(args)))
        return results

class DaskLocalExecutor(BaseExecutor):
    def __init__(self):
        from dask.distributed import LocalCluster, Client
        self.cluster = LocalCluster()
        self.client = Client(self.cluster)
        print("Created Dask LocalCluster()")

    def __del__(self):
        if hasattr(self, 'cluster') and self.cluster is not None:
            self.cluster.close()
            print("Closed Dask cluster")
        if hasattr(self, 'client') and self.client is not None:
            self.client.close()

    def _execute(self, func, args, kwargs):
        args_sc = self.client.scatter(args)
        futures = [self.client.submit(func, arg, kwargs) for arg in args_sc]
        results = self.client.gather(futures)
        results = list(results)
        return results

class DaskGatewayExecutor(BaseExecutor):
    def __init__(self):
        from dask_gateway import Gateway
        self.gateway = Gateway()
        self._find_gateway_client()

    def _find_gateway_client(self):
        clusters = self.gateway.list_clusters()
        if len(clusters)==0:
            raise Error("No Dask Gateway clusters found")

        first_cluster_name = clusters[0].name
        if len(clusters)>1:
            print(f"More than 1 Dask Gateway clusters found, will connect to the 1st one: {first_cluster_name}")

        self.cluster = self.gateway.connect(first_cluster_name)
        self.client = self.cluster.get_client()

    def _execute(self, func, args, kwargs):
        args_sc = self.client.scatter(args)
        futures = [self.client.submit(func, arg, kwargs) for arg in args_sc]
        results = self.client.gather(futures)
        results = list(results)
        return results


executors = {
    'sequential': SequentialExecutor,
    'futures': FuturesExecutor,
    'dask-local': DaskLocalExecutor,
    'dask-gateway': DaskGatewayExecutor
}



