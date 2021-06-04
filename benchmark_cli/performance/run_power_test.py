import subprocess

import mysql.connector
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG, ROOT_DIR, QUERIES_DIR
from benchmark_cli.performance.stream.QueryStream import QueryStream
from benchmark_cli.performance.stream.RefreshPair import RefreshPair
from benchmark_cli.performance.stream.RefreshStream import RefreshStream
from benchmark_cli.performance.utils import create_logger


def run_power_test():
    log = create_logger('power_test')
    log.info('Start power test...')

    # Generate queries
    subprocess.run([f'{ROOT_DIR}/generators/generate_refresh_data.sh'])
    subprocess.run([f'{ROOT_DIR}/generators/generate_queries.sh'])

    try:
        connection = MySQLConnection(**DB_CONFIG)
        cursor = connection.cursor(buffered=True)
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        raise

    refresh_pair = RefreshPair('refresh_pair_powertest', 1, connection, cursor)
    query_stream = QueryStream('query_stream_powertest', 0, connection, cursor)

    refresh_pair.load_data()
    query_stream.load_data()

    refresh_pair.execute_refresh_function1()
    query_stream.execute_stream()
    #refresh_pair.execute_refresh_function2()

    log.info(f'refresh_pair time:\n {refresh_pair.df_measures}')
    log.info(f"refresh_pair total_time: {refresh_pair.df_measures.at['total_time', 'time']}")
    log.info(f'query_stream time:\n {query_stream.df_measures}')
    log.info(f"query_stream total_time: {query_stream.df_measures.at['total_time', 'time']}")

    # query_stream = QueryStream('query_stream_0', 0)
    # query_stream.load_data()
    # # Run queries
    # query_stream.execute_stream()

    # refresh_stream = RefreshStream('refresh_stream_0', 1)
    # refresh_stream.load_data()
    # refresh_stream.execute_stream()

    # todo: Run refresh function 1
    # refresh_function_1(...)

    # todo: Run refresh function 2
    # refresh_function_2(...)
