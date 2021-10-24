import fire

from benchmark_cli.benchmark.BenchmarkRunner import BenchmarkRunner
from benchmark_cli.benchmark.DatabaseGenerator import DatabaseGenerator

def run_benchmark():
    benchmark = BenchmarkRunner()
    benchmark.run_benchmark()


def generate_database():
    generator = DatabaseGenerator()
    generator.reset_db()
    generator.generate_data()
    generator.load_db()
    generator.generate_refresh_data()


if __name__ == '__main__':
    """
    CLI to run workload_cmd scripts
    """

    fire.Fire()
