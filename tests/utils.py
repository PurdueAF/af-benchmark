import os, sys
sys.path.append(os.getcwd())
import copy
from src.benchmark import Benchmark

def run_tests(config, functions):
    b = Benchmark(config)
    for func in functions:
        old_config = copy.deepcopy(b.config)
        func(b)
        b.reset()
        b.config = old_config
    