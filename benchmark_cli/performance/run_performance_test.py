from benchmark_cli.performance import run_power_test, run_throughput_test
from benchmark_cli.performance.constants import SCALE_FACTOR
from benchmark_cli.performance.utils import create_logger


def run_performance_test(stream_count: int, run_number: int) -> float:
    log = create_logger(f'run_performance_test{run_number}')

    power_size = run_power_test((stream_count + 1) * run_number + 1)
    throughput_size = run_throughput_test(stream_count, (stream_count + 1) * run_number + 2)

    QphH = (power_size * throughput_size) ** (1/2)
    log.info(f'QphH@{SCALE_FACTOR}GB: {QphH}')
    return QphH
