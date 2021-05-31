import subprocess

import mysql.connector
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG, ROOT_DIR
from benchmark_cli.performance.stream.query_stream import query_stream
from benchmark_cli.performance.utils import create_logger


def power_test():
    log = create_logger('power_test')
    log.info('Start power test...')

    # Create session
    try:
        log.info('Trying to connect to database...')
        connection = MySQLConnection(**DB_CONFIG)
        # fetch result immediately after executing query
        cursor = connection.cursor(buffered=True)
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        return
    log.info('Database connected successful.')

    # Generate queries
    subprocess.run([f'{ROOT_DIR}/generators/generate_queries.sh'])

    # todo: Run refresh function 1
    # refresh_function_1(...)

    # Run queries
    query_stream(cursor, 0)

    # todo: Run refresh function 2
    # refresh_function_2(...)

    # Close
    cursor.close()
    connection.close()
