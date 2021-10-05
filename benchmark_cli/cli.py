import fire

from benchmark_cli.benchmark.BenchmarkRunner import BenchmarkRunner


def run_benchmark():
    benchmark = BenchmarkRunner()
    benchmark.run_benchmark()


if __name__ == '__main__':
    """
    CLI to run workload_cmd scripts
    """

    fire.Fire()
