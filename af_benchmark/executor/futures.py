from executor.base import BaseExecutor
from profiling.timing import time_profiler as tp
from concurrent import futures

class FuturesExecutor(BaseExecutor):
    """Futures executor

    Uses ``concurrent.futures`` to parallelize execution over CPU cores
    on the same node where the benchmark is launched.
    """

    @tp.enable
    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in parallel using ``concurrent.futures.ThreadPoolExecutor``.
        
        :meta public:
        """
        with futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(lambda arg: func(arg, **kwargs), args))
        return results