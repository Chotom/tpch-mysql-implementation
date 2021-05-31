"""Refresh functions methods to measure and execute insert/delete performance
"""
import datetime
import random
import subprocess

import mysql
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursorBuffered

from benchmark_cli.performance.constants import ROOT_DIR, DB_CONFIG, ORDER_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST
from benchmark_cli.performance.utils import create_logger, data_row_to_query


def refresh_function_stream():
    # todo: run refresh function1 and refresh function2

    return


def refresh_function_1(scale: float, connection: MySQLConnection, cursor: MySQLCursorBuffered, run_number: int):
    log = create_logger('refresh_function_1')
    rf1_stream_time = datetime.timedelta(0)

    # Generate refresh data - should be done before test
    subprocess.run([f'{ROOT_DIR}/generators/generate_refresh_data.sh'])

    # Get data from files and convert data rows to queries
    with open(f'/db_refresh_data/orders.tbl.u{run_number}', 'r') as orders_file:
        orders_queries = orders_file.readlines()
        for i, values_row in enumerate(orders_queries):
            orders_queries[i] = data_row_to_query(values_row, 'order', ORDER_QUOTE_INDEX_LIST)
        orders_queries_iterator = iter(orders_queries)

    with open(f'/db_refresh_data/lineitem.tbl.u{run_number}', 'r') as lineitem_file:
        lineitem_queries = lineitem_file.readlines()
        for i, values_row in enumerate(lineitem_queries):
            lineitem_queries[i] = data_row_to_query(values_row, 'lineitem', LINEITEM_QUOTE_INDEX_LIST)
        lineitem_queries_iterator = iter(lineitem_queries)

    log.info('Start refresh function 1...')
    for i in range(int(scale * 1500)):
        query = next(orders_queries_iterator)

        # Measure time for transaction
        start = datetime.datetime.now()

        # Insert new row into ORDERS table
        cursor.execute(f"{query}")
        for j in range(random.randint(1, 7)):
            # Insert new row into LINEITEM table
            cursor.execute(f"{next(lineitem_queries_iterator)}")
        connection.commit()

        measured_time = datetime.datetime.now() - start
        rf1_stream_time += measured_time

        # Print additional information about query
        log.debug(f'Time for {query} query: {measured_time}')


def refresh_function_2(scale: float):
    # todo: same as refresh_function_1

    for i in range(int(scale * 1500)):
        # DELETE FROM ORDERS WHERE O_ORDERKEY = [value] (from file /db_refresh_data/delete.{n})
        # DELETE FROM LINEITEM WHERE L_ORDERKEY = [value] (from file /db_refresh_data/delete.{n})
        pass
