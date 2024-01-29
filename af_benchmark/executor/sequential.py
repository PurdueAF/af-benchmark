from executor.base import BaseExecutor
from profiling.timing import time_profiler as tp


class SequentialExecutor(BaseExecutor):
    """Simple sequential executor

    Processes arguments in a ``for`` loop.
    """

    @tp.enable
    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in a loop.
        
        :meta public:
        """
        return [func(arg, **kwargs) for arg in args]