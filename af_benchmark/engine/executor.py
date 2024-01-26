from abc import ABC, abstractmethod
from profiling.timing import time_profiler as tp


class BaseExecutor(ABC):
    """A base class for a benchmark executor
    """

    @tp.enable
    def execute(self, func, args, **kwargs):
        """Executes a given function over a list or dict of arguments,
        and passes arbitrary keyword arguments to the function.
        The particular meaning of "execution" is defined in ``_execute()`` method,
        and implemented in inherited executor classes.

        Parameters
        ----------
        func : callable
            The function to be executed.
        args : list | dict
            A list (or dict) of input arguments for ``func``.
        kwargs : dict, optional
            Arbitrary keyword arguments passed to ``func``.

        Returns
        -------
        results: list | dict
            Results of the execution in the same format as input arguments.
        """

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

    @tp.enable
    @abstractmethod
    def _execute(self, func, args, kwargs):
        """Executor-specific implementation (see inherited classes)

        :meta public:
        """

        pass


class SequentialExecutor(BaseExecutor):
    """Simple sequential executor

    Processes arguments in a ``for`` loop.
    """

    @tp.enable
    def _execute(self, func, args, kwargs):
        """Execute ``func`` over ``args`` in a loop.
        
        :meta public:
        """
        return [func(arg, kwargs) for arg in args]

class FuturesExecutor(BaseExecutor):
    """Futures executor

    Uses ``concurrent.futures`` to parallelize execution over CPU cores
    on the same node where the benchmark is launched.
    """

    @tp.enable
    def _execute(self, func, args, kwargs):
        """Execute ``func`` over ``args`` in parallel using ``concurrent.futures.ThreadPoolExecutor``.
        
        :meta public:
        """
        from concurrent import futures
        with futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(func, args, [kwargs] * len(args)))
        return results

class DaskLocalExecutor(BaseExecutor):
    """Dask executor with a local cluster

    Creates a `LocalCluster <https://docs.dask.org/en/stable/deploying-python.html#localcluster>`_
    and uses it to parallelize execution over local CPU cores (same node as the benchmark)
    """

    @tp.enable
    def __init__(self):
        """Create a ``LocalCluster`` and a ``Client`` connected to it.

        :meta public:
        """

        from dask.distributed import LocalCluster, Client
        self.cluster = LocalCluster()
        self.client = Client(self.cluster)
        print("Created Dask LocalCluster()")

    @tp.enable
    def __del__(self):
        """Shut down the client and the cluster in the end of benchmarking.

        :meta public:
        """

        if hasattr(self, 'cluster') and self.cluster is not None:
            self.cluster.close()
            print("Closed Dask cluster")
        if hasattr(self, 'client') and self.client is not None:
            self.client.close()

    @tp.enable
    def _execute(self, func, args, kwargs):
        """Execute ``func`` over ``args`` in parallel using ``distributed.Client::submit()``.
        
        :meta public:
        """
        args_sc = self.client.scatter(args)
        futures = [self.client.submit(func, arg, kwargs) for arg in args_sc]
        results = self.client.gather(futures)
        results = list(results)
        return results

class DaskGatewayExecutor(BaseExecutor):
    """Dask Gateway executor

    Searches for an existing Gateway cluster and uses it to parallelize execution
    over multiple nodes using a batch system defined in Dask Gateway's backend (e.g. Slurm).
    """

    @tp.enable
    def __init__(self):
        from dask_gateway import Gateway
        self.gateway = Gateway()
        self._find_gateway_client()

    @tp.enable
    def _find_gateway_client(self):
        """Searches for an existing Dask Gateway cluster and connects to it automatically.

        If no Gateway clusters are found, an error is raised. If more than one Gateway cluster
        is found, connect to the first found one.
        """

        clusters = self.gateway.list_clusters()
        if len(clusters)==0:
            raise Error("No Dask Gateway clusters found")

        first_cluster_name = clusters[0].name
        if len(clusters)>1:
            print(f"More than 1 Dask Gateway clusters found, will connect to the 1st one: {first_cluster_name}")

        self.cluster = self.gateway.connect(first_cluster_name)
        self.client = self.cluster.get_client()

    @tp.enable
    def _execute(self, func, args, kwargs):
        """Execute ``func`` over ``args`` in parallel using ``distributed.Client::submit()``.
        
        :meta public:
        """
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



