from utils import run_tests


def test_processor_columns_explicit(b):
    b.config["processor"]["columns"]["method"] = "column_list"
    b.config["processor"]["columns"]["values"] = ["Muon_pt", "Muon_eta"]
    b.run()
    print(f"Successfully tested processing explicit list of columns")

def test_processor_columns_number(b):
    b.config["processor"]["columns"]["method"] = "n_columns"
    b.config["processor"]["columns"]["values"] = 5
    b.run()
    print(f"Successfully tested processing given number of columns")

def test_processor_collections(b):
    b.config["processor"]["columns"]["method"] = "collections"
    b.config["processor"]["collections"]["values"] = ["Muon"]
    b.run()
    print(f"Successfully tested processing a collection of columns")

def test_processor_dont_load(b):
    b.config["processor"]["load_into_memory"] = False
    b.run()
    print(f"Successfully tested doing nothing to specified columns")

def test_processor_load(b):
    b.config["processor"]["load_into_memory"] = True
    b.run()
    print(f"Successfully tested loading specified columns into memory")

def test_processor_worker_operation(b):
    b.config["processor"]["worker_operation_time"] = 10
    b.run()
    print(f"Successfully tested worker operation (10s)")

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
    b.config["processor"]["columns"]["method"] = "n_columns"
    b.config["processor"]["columns"]["values"] = 2
    b.run()
    print(f"Successfully tested parallelization over files")

def test_processor_parallelize_over_files_and_columns(b):
    b.config["processor"]["parallelize_over"] = "files_and_columns"
    b.config["executor"]["backend"] = "futures"
    b.config["data-access"]["files"] = [
        "tests/data/nano_dimuon.root",
        "tests/data/nano_dimuon.root"
    ]
    b.config["processor"]["columns"]["method"] = "n_columns"
    b.config["processor"]["columns"]["values"] = 2
    b.run()
    print(f"Successfully tested parallelization over files and columns")


if __name__=='__main__':
    run_tests(
        config="tests/config-default.yaml",
        functions=[
            test_processor_columns_explicit,
            test_processor_columns_number,
            test_processor_collections,
            test_processor_dont_load,
            test_processor_load,
            test_processor_worker_operation,
            test_processor_parallelize_over_files,
            test_processor_parallelize_over_columns,
            test_processor_parallelize_over_files_and_columns,
        ]
    )

