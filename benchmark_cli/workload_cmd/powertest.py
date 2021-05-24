import mysql.connector
import subprocess
from mysql.connector import MySQLConnection

from benchmark_cli.workload_cmd._utils import create_logger, run_query_stream, ROOT_DIR, DB_CONFIG


def power_test():
    log = create_logger('power_test')
    log.info('Start power test...')

    # Create session
    try:
        log.info('Trying to connect to database...')
        connection = MySQLConnection(**DB_CONFIG)
        cursor = connection.cursor(buffered=True)
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        return
    log.info('Database connected successful.')

    # Generate queries
    subprocess.run([f'{ROOT_DIR}/generators/generate_queries.sh'])

    # Run queries
    run_query_stream(cursor, 0)

    # Close
    cursor.close()
    connection.close()
