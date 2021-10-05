import numpy as np
import pandas as pd

from benchmark_cli.constants import RESULTS_DIR, SCALE_FACTOR
from benchmark_cli.benchmark.stream.QueryStream import QueryStream
from benchmark_cli.benchmark.stream.RefreshPair import RefreshPair
from benchmark_cli.utils import create_logger, get_connection, get_timestamp


def run_power_test(refresh_file_start_index: int, timestamp: str) -> float:
    """
    Execute refresh pair 1, query stream and refresh pair 2 in sequence to
    measure database benchmark for single user and saves measurements.

    :param refresh_file_start_index: index of file with refresh data to execute
    :param timestamp:
    :return: calculated power size measure
    """
    log = create_logger('power_test')
    log.info('Start power test...')

    file_path = f'{RESULTS_DIR}/power_test_{timestamp}.csv'
    connection, cursor = get_connection(log, True)
    refresh_pair = RefreshPair('refresh_pair_powertest', refresh_file_start_index, connection, cursor)
    query_stream = QueryStream('query_stream_powertest', 0, connection, cursor)

    refresh_pair.load_data()
    query_stream.load_data()

    refresh_pair.execute_refresh_function1()
    query_stream.execute_stream()
    refresh_pair.execute_refresh_function2()

    power_test_results = query_stream.df_measures.append(refresh_pair.df_measures)
    power_test_results.to_csv(file_path)
    log.info(f'Power test measurements saved to: {file_path}')

    return _calculate_power_size(power_test_results)


def _calculate_power_size(power_test_results: pd.DataFrame):
    geometric_mean = np.prod(power_test_results['time'].apply(lambda x: x.total_seconds())) ** (1 / 24)
    power_size = 3600 * SCALE_FACTOR / geometric_mean

    return power_size
