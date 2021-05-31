
from multiprocessing import Process, Queue

from benchmark_cli.performance.utils import create_logger


def run_throughput_test():
    log = create_logger('throughtput_test')
    log.info('Start throughput test...')

    num_streams = 5
    processes = []
    queue = Queue()

    for i in range(num_streams):
        stream = i + 1

        log.info(f'Run throughput test for stream {stream}')

        # todo: Run query stream in parallel
        #processes.append(Process(target=, args=stream))
        processes[i].start()

        # todo: Run refresh stream in parallel with query stream