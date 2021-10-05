import pandas as pd

from benchmark_cli.ConfigEditor import ConfigEditor
from benchmark_cli.constants import MAX_REFRESH_FILE_INDEX, SCALE_FACTOR, RESULTS_DIR, CONFIG_FILE_PATH
from benchmark_cli.benchmark import run_power_test, run_throughput_test
from benchmark_cli.benchmark.generate_data import generate_data
from benchmark_cli.utils import create_logger, get_timestamp


class BenchmarkRunner(ConfigEditor):
    """
    A class for benchmark attributes and methods to executes performance test
    and saves measurement results in data files.
    """

    def __init__(self, config_file_path: str = CONFIG_FILE_PATH):
        super().__init__(config_file_path)
        self._log = create_logger('benchmark_tpc_h')
        self._timestamp = get_timestamp()
        self._result_file_path = f'{RESULTS_DIR}/benchmark_results_{get_timestamp()}.csv'
        generate_data(self._stream_count)

    def run_benchmark(self):
        """Execute two performance tests and saves the results of second test (the one with better accuracy.)"""
        self._log.info(f'Run benchmark with {self._timestamp} timestamp.')
        for i in range(2):
            self._log.info(f'Performance test {i} started...')
            self.run_performance_test()
            self._log.info(f'End of benchmark test {i}. Saved results to {self._result_file_path}.')
        self._save_config()

    def run_performance_test(self):
        """Execute in sequence power and throughput test and saves the results."""
        power_size = run_power_test(self._refresh_file_index, self._timestamp)
        self.__inc_refresh_file_index()
        throughput_size = run_throughput_test(self._stream_count, self._refresh_file_index, self._timestamp)
        self.__inc_refresh_file_index(self._stream_count)
        qph_h = (power_size * throughput_size) ** (1 / 2)

        results = {
            f'Power@{SCALE_FACTOR}GB': [power_size],
            f'Throughput@{SCALE_FACTOR}GB': [throughput_size],
            f'QphH@{SCALE_FACTOR}GB': [qph_h],
        }
        pd.DataFrame.from_dict(results).to_csv(self._result_file_path, index=False)

    def __inc_refresh_file_index(self, number: int = 1):
        self._refresh_file_index = (self._refresh_file_index + number) % MAX_REFRESH_FILE_INDEX
