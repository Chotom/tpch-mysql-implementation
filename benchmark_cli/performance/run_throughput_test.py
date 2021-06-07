import datetime
from threading import Thread

import pandas as pd

from benchmark_cli.performance.constants import ROOT_DIR, RESULTS_DIR, SCALE_FACTOR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_throughput_test(stream_count: int, refresh_file_start_index: int = 2) -> float:
    log = create_logger('throughput_test')
    log.info('Start throughput test...')

    streams = []
    processes = []
    streams.append(RefreshStream('refresh_stream', stream_count, refresh_file_start_index))

    if stream_count < 1:
        log.error(f'Stream number cannot be 0')
        raise

    for i in range(stream_count):
        streams.append(QueryStream(f'query_stream_{i + 1}', i + 1))

    # Load data for all streams
    for i in range(len(streams)):
        streams[i].load_data()
        processes.append(Thread(target=streams[i].execute_stream))

    start = datetime.datetime.now()

    # Execute streams in parallel
    for i in range(len(streams)):
        processes[i].start()

    # Wait for all processes to end
    for proc in processes:
        proc.join()

    total_time = max(stream.df_measures.at['total_time', 'time'] for stream in streams)

    log.info(f'Refresh stream time:\n {streams[0].df_measures}\n\n')
    for i, stream in enumerate(streams[1:]):
        log.info(f'Stream {i + 1} time:\n {stream.df_measures}\n\n')
        if i == 0:
            df_throughput_test_results = stream.df_measures
        else:
            df_throughput_test_results = df_throughput_test_results.merge(stream.df_measures, suffixes=('', f'_{i}'), on='name')
    log.info(f'Throughput test ended for {stream_count} streams. Total time: {total_time}')
    df_throughput_test_results.to_csv(f'{RESULTS_DIR}/throughput_test.csv')

    throughput_size = stream_count * 22 * 3600 * SCALE_FACTOR / total_time.total_seconds()
    log.info(f'Throughput@{SCALE_FACTOR}GB: {throughput_size}')
    return throughput_size
