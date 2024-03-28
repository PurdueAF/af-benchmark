from abc import ABC, abstractmethod
import time

class BaseExecutor(ABC):
    """A base class for a benchmark executor
    """
    
    def __init__(self, **kwargs):
        return

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

        results = self._execute(func, args, **kwargs) 

        if is_dict:
            results = dict(zip(keys, results))

        return results

    @abstractmethod
    def _execute(self, func, args, **kwargs):
        """Executor-specific implementation (see inherited classes)

        :meta public:
        """

        return

    
    def wait_for_workers(self, nworkers):
        if not hasattr(self, "cluster"):
            return
        self.cluster.scale(nworkers)
        while (
            (self.cluster.status == "running") and 
            (len(self.cluster.scheduler_info["workers"]) != nworkers)
        ):
            time.sleep(1.0)
        return


    @abstractmethod
    def get_n_workers(self):
        return
