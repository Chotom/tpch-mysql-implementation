import subprocess
from threading import Thread

from benchmark_cli.performance.constants import ROOT_DIR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_throughput_test(stream_number: int, refresh_file_start_index: int = 2):
    log = create_logger('throughtput_test')
    log.info('Start throughput test...')

    # # Generate queries
    # subprocess.run([f'{ROOT_DIR}/generators/generate_refresh_data.sh'])
    # subprocess.run([f'{ROOT_DIR}/generators/generate_queries.sh'])

    streams = []
    processes = []
    streams.append(RefreshStream('refresh_stream', stream_number, refresh_file_start_index))

    for i in range(stream_number):
        streams.append(QueryStream(f'query_stream_{i + 1}', i + 1))

    # Load data for all streams
    for i in range(len(streams)):
        streams[i].load_data()
        processes.append(Thread(target=streams[i].execute_stream))

    # Execute streams in parallel
    for i in range(len(streams)):
        processes[i].start()

    # Wait for all processes to end
    for proc in processes:
        proc.join()

    log.info(f'Refresh stream time:\n {streams[0].df_measures}\n\n')
    for i in range(len(streams[1:])):
        log.info(f'Stream {i} time:\n {streams[i].df_measures}\n\n')

    total_time = 0
    for stream in streams:
        total_time += stream.df_measures.at['total_time', 'time']
    log.info(f'Throughput test ended for {stream_number} streams. Total time: {total_time}')
