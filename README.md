[![Documentation Status](https://readthedocs.org/projects/af-benchmark/badge/?version=latest)](https://af-benchmark.readthedocs.io/en/latest/?badge=latest)
![CI/CD](https://github.com/PurdueAF/af-benchmark/actions/workflows/ci.yml/badge.svg)

# Analysis Facility Benchmark

[https://af-benchmark.readthedocs.io](https://af-benchmark.readthedocs.io/en/latest/)

🏗️ Work in progress 🚧


This benchmark is designed for generic but comprehensive performance tests of the computing infrastructure at CMS Analysis Facilities. It currently includes the following functionality:

- Multiple options for code execution:
    - Sequential
    - Parallelized via `concurrent.futures`
    - Parallelized via `Dask` using local cluster
    - Parallelized via `Dask` using Gateway cluster
- Multiple methods of loading and reading columns from NanoAOD ROOT files:
    - `uproot`
    - `coffea.nanoevents`
- Generic operations applied to data in columns:
    - Nothing (just open the ROOT file with a given method)
    - Load column data into memory
    - Perform a simple operation on a column, e.g. `mean()`
- Time profiling
- Measuring size of columns in bytes


