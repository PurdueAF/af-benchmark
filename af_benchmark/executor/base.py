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

        results = self._execute(func, args, **kwargs) 

        if is_dict:
            results = dict(zip(keys, results))

        return results

    @tp.enable
    @abstractmethod
    def _execute(self, func, args, **kwargs):
        """Executor-specific implementation (see inherited classes)

        :meta public:
        """

        return

    @abstractmethod
    def get_n_workers(self):
        return
