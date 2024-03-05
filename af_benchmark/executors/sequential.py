from executors.base import BaseExecutor


class SequentialExecutor(BaseExecutor):
    """Simple sequential executor

    Processes arguments in a ``for`` loop.
    """

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in a loop.
        
        :meta public:
        """
        first = [func(arg, **kwargs) for arg in args]
        return [func(arg, **kwargs) for arg in args]

        print(first)
        second=2
        print(second)

    def get_n_workers(self):
        return 1
