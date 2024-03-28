from src.executors.base import BaseExecutor
from concurrent import futures

class FuturesExecutor(BaseExecutor):
    """Futures executor

    Uses ``concurrent.futures`` to parallelize execution over CPU cores
    on the same node where the benchmark is launched.
    """

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in parallel using ``concurrent.futures.ThreadPoolExecutor``.
        
        :meta public:
        """
        with futures.ThreadPoolExecutor() as executor:
            self.max_workers = executor._max_workers
            results = list(executor.map(lambda arg: func(arg, **kwargs), args))
        return results

    def get_n_workers(self):
        return self.max_workers
