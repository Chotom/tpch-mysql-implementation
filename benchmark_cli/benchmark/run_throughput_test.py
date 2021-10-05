from threading import Thread

import pandas as pd

from benchmark_cli.constants import RESULTS_DIR, SCALE_FACTOR
from benchmark_cli.benchmark.stream.QueryStream import QueryStream
from benchmark_cli.benchmark.stream.RefreshStream import RefreshStream
from benchmark_cli.utils import create_logger


def run_throughput_test(stream_count: int, refresh_file_start_index: int, timestamp: str) -> float:
    """
    Execute refresh stream and given number of query streams in parallel threads and
    measure database benchmark for single user and saves measurements.

    :param stream_count: number of parallel streams
    :param refresh_file_start_index: index of file with refresh data to execute
    :param timestamp:
    :return: calculated throughput size measure
    """
    log = create_logger('throughput_test')
    log.info('Start throughput test...')

    file_path = f'{RESULTS_DIR}/throughput_test_{timestamp}.csv'
    processes = []
    streams = [RefreshStream('refresh_stream', stream_count, refresh_file_start_index)]
    for i in range(stream_count):
        streams.append(QueryStream(f'query_stream_{i + 1}', i + 1))

    for stream in streams:
        stream.load_data()
        processes.append(Thread(target=stream.execute_stream))

    # Execute streams in parallel threads
    for execute_stream_process in processes:
        execute_stream_process.start()

    # Wait for all processes to end
    for proc in processes:
        proc.join()

    _save_throughput_test_results(streams, file_path)
    log.info(f'Power test measurements saved to: {file_path}')

    return _calculate_throughput_size(streams)


def _save_throughput_test_results(streams: list, file_path: str):
    for i, stream in enumerate(streams[1:]):
        measures = stream.df_measures.copy()
        measures.loc['RF1'] = [streams[0].df_measures.at[f'RF1_{i}', 'time']]
        measures.loc['RF2'] = [streams[0].df_measures.at[f'RF2_{i}', 'time']]
        stream.df_measures = measures

    results = pd.DataFrame
    for i, stream in enumerate(streams[1:]):
        if results.empty:
            results = stream.df_measures
        else:
            results = results.merge(stream.df_measures, suffixes=('', f'_{i}'), on='name')
    results.to_csv(file_path)


def _calculate_throughput_size(streams: list):
    total_time = max(stream.df_measures['time'].sum() for stream in streams)
    throughput_size = (len(streams) - 1) * 22 * 3600 * SCALE_FACTOR / total_time.total_seconds()

    return throughput_size
