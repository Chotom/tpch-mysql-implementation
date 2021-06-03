import datetime
from typing import Iterator

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursorBuffered, MySQLCursor

from benchmark_cli.performance.constants import ORDERS_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST, REFRESH_DATA_DIR
from benchmark_cli.performance.utils import data_row_to_query, create_logger


class RefreshPair():
    __queries_iter: Iterator[str]

    def __init__(self, logger_name: str, run_number: int, connection: MySQLConnection, cursor: MySQLCursorBuffered):
        self._log = create_logger(logger_name)
        self._connection = connection
        self._cursor = cursor
        self.__run_number = run_number
        self.__rf1_time = datetime.timedelta(0)
        self.__rf2_time = datetime.timedelta(0)

    def load_data(self):
        self._log.info('Load queries...')

        queries = []

        with open(f'{REFRESH_DATA_DIR}/orders.tbl.u{self.__run_number}', 'r') as orders_file,\
                open(f'{REFRESH_DATA_DIR}/lineitem.tbl.u{self.__run_number}', 'r') as lineitem_file:
            try:
                # read first lineitem query
                lineitem_row = next(lineitem_file)
                lineitem_id, lineitem_query = data_row_to_query(lineitem_row, 'lineitem', LINEITEM_QUOTE_INDEX_LIST)
            except StopIteration:
                raise Exception("Lineitem update file is empty.")

            for orders_row in orders_file:
                # read orders query
                orders_id, orders_query = data_row_to_query(orders_row, 'orders', ORDERS_QUOTE_INDEX_LIST)
                queries.append(orders_query)

                # if current orders query has lineitem children
                while orders_id == lineitem_id:
                    # add lineitem
                    queries[-1] += lineitem_query

                    try:
                        # read next lineitem query
                        lineitem_row = next(lineitem_file)
                        lineitem_id, lineitem_query = data_row_to_query(lineitem_row, 'lineitem',
                                                                        LINEITEM_QUOTE_INDEX_LIST)
                    except StopIteration:
                        break

        # save queries
        self.__queries_iter = iter(queries)

        self._log.info('Queries loaded successfully...')

    def execute_pair(self):
        self._log.info('Run refresh stream...')

        self.execute_refresh_function1()
        self.execute_refresh_function2()

        self._log.info(f'Execution of refresh stream ended successful. Measured time: {self._measured_total_time}')

    def execute_refresh_function1(self):
        self._log.info('Run refresh function 1...')

        # Execute insert queries
        for query in self.__queries_iter:
            self._log.debug(f'Query: {query}')
            # Insert new row into `orders` and its `lineitem` table
            cursors_generator = self._cursor.execute(query, multi=True)

            # Measure time for transaction
            start = datetime.datetime.now()

            # for _ in cursors_generator: pass    # iterate over generated cursors to execute them and get the results
            cursors = [cur for cur in cursors_generator]

            self._connection.commit()

            time_delta = datetime.datetime.now() - start
            self.__rf1_time += time_delta

            # Print additional information
            self._log.debug(f'Time for {query} query: {time_delta}')

        # todo: correct line below
        # self._df_measures.append({'name': f'RF1', 'time': self.__rf1_time}, ignore_index=True)
        self._log.info(f'Execution of refresh function 1 ended successful. Measured time: {self.__rf1_time}')

    def execute_refresh_function2(self):
        return NotImplemented
