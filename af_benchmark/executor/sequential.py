from executor.base import BaseExecutor
import tqdm


class SequentialExecutor(BaseExecutor):
    """Simple sequential executor

    Processes arguments in a ``for`` loop.
    """

    def _execute(self, func, args, **kwargs):
        """Execute ``func`` over ``args`` in a loop.
        
        :meta public:
        """
        return [func(arg, **kwargs) for arg in tqdm.tqdm(args)]

    def get_n_workers(self):
        return 1