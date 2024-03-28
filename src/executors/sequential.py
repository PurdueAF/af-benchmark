from executors.base import BaseExecutor


class SequentialExecutor(BaseExecutor):
    """Simple sequential executor

    Processes arguments in a ``for`` loop.
    """

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in a loop.
        
        :meta public:
        """
        return [func(arg, **kwargs) for arg in args]

    def get_n_workers(self):
        return 1
