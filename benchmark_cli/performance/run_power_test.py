import subprocess

import numpy as np

import mysql.connector
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG, ROOT_DIR, QUERIES_DIR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshPair import RefreshPair
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_power_test(refresh_file_start_index: int = 1):
    log = create_logger('power_test')
    log.info('Start power test...')

    # Generate queries
    subprocess.run([f'{ROOT_DIR}/generators/generate_queries.sh'])

    try:
        connection = MySQLConnection(**DB_CONFIG)
        cursor = connection.cursor(buffered=True)
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        raise

    refresh_pair = RefreshPair('refresh_pair_powertest', refresh_file_start_index, connection, cursor)
    query_stream = QueryStream('query_stream_powertest', 0, connection, cursor)

    refresh_pair.load_data()
    query_stream.load_data()

    refresh_pair.execute_refresh_function1()
    query_stream.execute_stream()
    refresh_pair.execute_refresh_function2()

    log.info(f'refresh_pair time:\n {refresh_pair.df_measures}')
    log.info(f"refresh_pair total_time: {refresh_pair.df_measures.at['total_time', 'time']}")
    log.info(f'query_stream time:\n {query_stream.df_measures}')
    log.info(f"query_stream total_time: {query_stream.df_measures.at['total_time', 'time']}")

    # calculate power@size
    SCALE_FACTOR = 0.1
    geometric_mean = np.prod(query_stream.df_measures.drop(['total_time'])['time'].apply(lambda x: x.total_seconds()))\
                     * np.prod(refresh_pair.df_measures.drop(['total_time'])['time'].apply(lambda x: x.total_seconds()))\
                     ** (1 / 24)
    power_size = 3600 * SCALE_FACTOR / geometric_mean
    log.info(f'Power@Size: {power_size}')
