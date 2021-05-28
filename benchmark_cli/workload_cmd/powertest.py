import mysql.connector
import subprocess
import random
from mysql.connector import MySQLConnection

from benchmark_cli.workload_cmd._utils import create_logger, run_query_stream, ROOT_DIR, DB_CONFIG,\
    generate_insert_into_orders_query, generate_insert_into_lineitem_query


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

    # Run queries
    run_query_stream(cursor, 0)

    # Close
    cursor.close()
    connection.close()


def refresh_function_1(scale: float, run_number: int):
    log = create_logger('refresh_function_1')
    log.info('Start refresh function 1...')

    # Generate refresh data - should be done before test
    subprocess.run([f'{ROOT_DIR}/generators/generate_refresh_data.sh'])

    # Get data from files and generate queries
    with open(f'/db_refresh_data/orders.tbl.u{run_number}', 'r') as orders_file:
        orders_queries = orders_file.readlines()
        for i, values_row in enumerate(orders_queries):
            orders_queries[i] = generate_insert_into_orders_query(values_row)
        orders_queries_iterator = iter(orders_queries)

    with open(f'/db_refresh_data/lineitem.tbl.u{run_number}', 'r') as lineitem_file:
        lineitem_queries = lineitem_file.readlines()
        for i, values_row in enumerate(lineitem_queries):
            lineitem_queries[i] = generate_insert_into_lineitem_query(values_row)
        lineitem_queries_iterator = iter(lineitem_queries)

    # Create session
    try:
        log.info('Trying to connect to database...')
        connection = MySQLConnection(**DB_CONFIG)
        # disable auto commit mode
        # disabled by default according to documentation:
        # https://dev.mysql.com/doc/connector-python/en/connector-python-api-mysqlconnection-autocommit.html
        # connection.autocommit = False
        cursor = connection.cursor()
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        return
    log.info('Database connected successful.')

    # Run queries
    for i in range(int(scale * 1500)):
        query = next(orders_queries_iterator)

        log.debug(query)
        # insert new row into ORDERS table
        cursor.execute(query)
        for j in range(random.randint(1, 7)):
            # insert new row into LINEITEM table
            cursor.execute(next(lineitem_queries_iterator))

        cursor.commit()

    # Close
    cursor.close()
    connection.close()


def refresh_function_2(scale: float):
    # todo: same as refresh_function_1

    for i in range(int(scale * 1500)):
        # DELETE FROM ORDERS WHERE O_ORDERKEY = [value] (from file /db_refresh_data/delete.{n})
        # DELETE FROM LINEITEM WHERE L_ORDERKEY = [value] (from file /db_refresh_data/delete.{n})
        pass
