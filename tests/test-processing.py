from utils import run_tests


def test_processor_columns_explicit(b):
    b.config["processor"]["columns"] = ["Muon_pt", "Muon_eta"]
    b.run()
    print(f"Successfully tested processing explicit list of columns")

def test_processor_columns_number(b):
    b.config["processor"]["columns"] = 5
    b.run()
    print(f"Successfully tested processing given number of columns")

def test_processor_collections(b):
    b.config["processor"]["columns"] = []
    b.config["processor"]["collections"] = ["Muon"]
    b.run()
    print(f"Successfully tested processing a collection of columns")

def test_processor_operation_nothing(b):
    b.config["processor"]["operation"] = "nothing"
    b.run()
    print(f"Successfully tested doing nothing to specified columns")

def test_processor_operation_load(b):
    b.config["processor"]["operation"] = "load_into_memory"
    b.run()
    print(f"Successfully tested loading specified columns into memory")

def test_processor_parallelize_over_files(b):
    b.config["processor"]["parallelize_over"] = "files"
    b.config["executor"]["backend"] = "futures"
    b.config["data-access"]["files"] = [
        "tests/data/nano_dimuon.root",
        "tests/data/nano_dimuon.root"
    ]
    b.run()
    print(f"Successfully tested parallelization over files")

def test_processor_parallelize_over_columns(b):
    b.config["processor"]["parallelize_over"] = "columns"
    b.config["executor"]["backend"] = "futures"
    b.config["processor"]["columns"] = 2
    b.run()
    print(f"Successfully tested parallelization over files")

def test_processor_parallelize_over_files_and_columns(b):
    b.config["processor"]["parallelize_over"] = "files_and_columns"
    b.config["executor"]["backend"] = "futures"
    b.config["data-access"]["files"] = [
        "tests/data/nano_dimuon.root",
        "tests/data/nano_dimuon.root"
    ]
    b.config["processor"]["columns"] = 2
    b.run()
    print(f"Successfully tested parallelization over files and columns")


if __name__=='__main__':
    run_tests(
        config="tests/config-default.yaml",
        functions=[
            test_processor_columns_explicit,
            test_processor_columns_number,
            test_processor_collections,
            test_processor_operation_nothing,
            test_processor_operation_load,
            test_processor_parallelize_over_files,
            test_processor_parallelize_over_columns,
            test_processor_parallelize_over_files_and_columns,
        ]
    )

