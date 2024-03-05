from utils import run_tests


def test_data_access_explicit_files(b):
    b.config["data-access"]["mode"] = "explicit-files"
    b.config["data-access"]["files"] = ["tests/data/nano_dimuon.root"]
    b.run()
    print(f"Successfully tested accessing data via explicing list of files")

def test_data_access_explicit_dirs(b):
    b.config["data-access"]["mode"] = "explicit-dirs"
    b.config["data-access"]["directories"] = ["tests/data/"]
    b.run()
    print(f"Successfully tested accessing data via explicing list of directories")


if __name__=='__main__':
    run_tests(
        config="tests/config-default.yaml",
        functions=[
            test_data_access_explicit_files,
            test_data_access_explicit_dirs,
        ]
    )

