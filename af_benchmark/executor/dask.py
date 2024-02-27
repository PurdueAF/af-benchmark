from executor.base import BaseExecutor
import dask
from dask.distributed import LocalCluster, Client
from dask_gateway import Gateway


class DaskLocalExecutor(BaseExecutor):
    """Dask executor with a local cluster

    Creates a `LocalCluster <https://docs.dask.org/en/stable/deploying-python.html#localcluster>`_
    and uses it to parallelize execution over local CPU cores (same node as the benchmark)
    """

    def __init__(self, **kwargs):
        """Create a ``LocalCluster`` and a ``Client`` connected to it.

        :meta public:
        """

        # disable GPU diagnostics to prevent Dask from crashing
        dask.config.set({"distributed.diagnostics.nvml": False})
        self.cluster = LocalCluster()
        self.client = Client(self.cluster)
        n_workers = kwargs.get("n_workers", 1)
        self.wait_for_workers(n_workers)

    def __del__(self):
        """Shut down the client and the cluster in the end of benchmarking.

        :meta public:
        """

        if hasattr(self, 'cluster') and self.cluster is not None:
            self.cluster.close()
        if hasattr(self, 'client') and self.client is not None:
            self.client.close()

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in parallel using ``distributed.Client::submit()``.
        
        :meta public:
        """
        #scattering is not working well
        #args_sc = self.client.scatter(args)
        args_sc = args
        futures = [self.client.submit(func, arg, **kwargs) for arg in args_sc]
        results = self.client.gather(futures)
        results = list(results)
        return results

    def get_n_workers(self):
        return len(self.client.scheduler_info()['workers'])

class DaskGatewayExecutor(BaseExecutor):
    """Dask Gateway executor

    Searches for an existing Gateway cluster and uses it to parallelize execution
    over multiple nodes using a batch system defined in Dask Gateway's backend (e.g. Slurm).
    """

    def __init__(self, **kwargs):
        self.gateway = Gateway()
        self._find_gateway_client()
        n_workers = kwargs.get("n_workers", 1)
        self.wait_for_workers(n_workers)


    def _find_gateway_client(self):
        """Searches for an existing Dask Gateway cluster and connects to it automatically.

        If no Gateway clusters are found, an error is raised. If more than one Gateway cluster
        is found, connect to the first found one.
        """

        clusters = self.gateway.list_clusters()
        if len(clusters)==0:
            raise Exception("No Dask Gateway clusters found")

        first_cluster_name = clusters[0].name
        if len(clusters)>1:
            print(f"More than 1 Dask Gateway clusters found, will connect to the 1st one: {first_cluster_name}")

        self.cluster = self.gateway.connect(first_cluster_name)
        self.client = self.cluster.get_client()

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in parallel using ``distributed.Client::submit()``.
        
        :meta public:
        """
        #scattering is not working well
        #args_sc = self.client.scatter(args)
        args_sc = args
        futures = [self.client.submit(func, arg, **kwargs) for arg in args_sc]
        results = self.client.gather(futures)
        results = list(results)
        return results

    def get_n_workers(self):
        return len(self.client.scheduler_info()['workers'])
