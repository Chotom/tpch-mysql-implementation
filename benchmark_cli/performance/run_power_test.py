import subprocess

import mysql.connector
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG, ROOT_DIR, QUERIES_DIR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_power_test():
    log = create_logger('power_test')
    log.info('Start power test...')

    # Generate queries
    subprocess.run([f'{ROOT_DIR}/generators/generate_refresh_data.sh'])

    # query_stream = QueryStream('query_stream_0', 0)
    # query_stream.load_data()
    # # Run queries
    # query_stream.execute_stream()

    refresh_stream = RefreshStream('refresh_stream_0', 1)
    refresh_stream.load_data()
    refresh_stream.execute_stream()

    # todo: Run refresh function 1
    # refresh_function_1(...)

    # todo: Run refresh function 2
    # refresh_function_2(...)
