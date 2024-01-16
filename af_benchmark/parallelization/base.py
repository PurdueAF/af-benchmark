def maybe_parallelize(cls, func, args, kwargs):
    params = cls.parameters.get('parallelization', {})
    do_parallelize = params.get('parallelize', False)
    backend = params.get('backend', None)

    if not do_parallelize:
        results = [func(arg, kwargs) for arg in args]
    elif backend == 'futures':
        from concurrent import futures
        with futures.ThreadPoolExecutor() as executor:
            results = list(executor.map(func, args, [kwargs] * len(args)))
    elif backend == 'dask-local':
        args_sc = cls.dask_client.scatter(args)
        dask_futures = [cls.dask_client.submit(func, arg, kwargs) for arg in args_sc]
        results = cls.dask_client.gather(dask_futures)
        results = list(results)
    else:
        raise NotImplementedError(f"Parallelization using {backend} backend is not implemented.")

    return results
