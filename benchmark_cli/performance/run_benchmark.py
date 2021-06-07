import subprocess

from benchmark_cli.performance.constants import ROOT_DIR
from benchmark_cli.performance.run_performance_test import run_performance_test
from benchmark_cli.performance.generate_data import generate_data


def run_benchmark(stream_count: int):
    generate_data(stream_count)

    for i in range(2):
        run_performance_test(stream_count, i)
