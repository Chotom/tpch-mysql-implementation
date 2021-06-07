import subprocess

import numpy as np

import mysql.connector
import pandas as pd
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG, ROOT_DIR, QUERIES_DIR, RESULTS_DIR, SCALE_FACTOR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshPair import RefreshPair
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_power_test(refresh_file_start_index: int = 1) -> float:
    log = create_logger('power_test')
    log.info('Start power test...')

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
    df_power_test_results: pd.DataFrame = query_stream.df_measures.drop(['total_time']).append(refresh_pair.df_measures.drop(['total_time']))
    df_power_test_results.to_csv(f'{RESULTS_DIR}/power_test.csv')

    # calculate power@size
    geometric_mean = np.prod(df_power_test_results['time'].apply(lambda x: x.total_seconds())) ** (1 / 24)
    power_size = 3600 * SCALE_FACTOR / geometric_mean
    log.info(f'Power@{SCALE_FACTOR}GB: {power_size}')

    return power_size
