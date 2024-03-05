from utils import run_tests


def test_executor_sequential(b):
    b.config["executor"]["backend"] = "sequential"
    b.run()
    print(f"Successfully tested sequential executor")

def test_executor_futures(b):
    b.config["executor"]["backend"] = "futures"
    b.run()
    print(f"Successfully tested futures executor")

def test_executor_dask_local(b):
    b.config["executor"]["backend"] = "dask-local"
    b.config["executor"]["workers"] = 1
    b.run()
    print(f"Successfully tested dask-local executor")


if __name__=='__main__':
    run_tests(
        config="tests/config-default.yaml",
        functions=[
            test_executor_sequential,
            test_executor_futures,
            test_executor_dask_local
        ]
    )


