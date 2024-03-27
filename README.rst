.. image:: https://readthedocs.org/projects/af-benchmark/badge/?version=latest
    :target: https://af-benchmark.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://github.com/PurdueAF/af-benchmark/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/PurdueAF/af-benchmark/actions/workflows/ci.yml/badge.svg
    :alt: CI Status


Analysis Facility Benchmark
============================

|docs_link|

.. |docs_link| raw:: html

   <a href="https://af-benchmark.readthedocs.io" target="_blank">
      https://af-benchmark.readthedocs.io
   </a>

.. start-badge

**🏗️ Work in progress 🚧**


This benchmark is designed for generic but comprehensive performance tests of the computing infrastructure at CMS Analysis Facilities. It currently includes the following functionality:

* Multiple options for code execution:

  * Sequential
  * Parallelized via ``concurrent.futures``
  * Parallelized via ``Dask`` using local cluster
  * Parallelized via ``Dask`` using Gateway cluster

* Multiple data access options:

  * Explicit list of files or directories in local or mounted filesystem
  * List of files, blocks, or datasets at CMS DBS accessed via network, e.g. XRootD or XCache

* Loading and reading columns from NanoAOD ROOT files is done using ``uproot``.

* Abstract "operation" with a given timeout executed by workers to emulate data processing during analysis. 

* Time profiling
* Measuring size of columns in bytes

.. end-badge

