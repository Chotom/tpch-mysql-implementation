import datetime
import random
from typing import Iterator

from benchmark_cli.performance.constants import ORDERS_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST
from benchmark_cli.performance.stream.AbstractStream import AbstractStream
from benchmark_cli.performance.utils import data_row_to_query


class RefreshStream(AbstractStream):
    __orders_queries_iter: Iterator[str]
    __lineitem_queries_iter: Iterator[str]

    def __init__(self, logger_name: str, data_path: str, is_buffered: bool, run_number: int, scale: float):
        super().__init__(logger_name, data_path, is_buffered)
        self.__run_number = run_number
        self.__scale = scale
        self.__rf1_time = datetime.timedelta(0)
        self.__rf2_time = datetime.timedelta(0)

    def load_data(self):
        self._log.info('Load queries...')

        queries = []

        with open(f'{self._data_path}/orders.tbl.u{self.__run_number}', 'r') as orders_file,\
                open(f'{self._data_path}/lineitem.tbl.u{self.__run_number}', 'r') as lineitem_file:
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

    def execute_stream(self):
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
        self._df_measures.append({'name': f'RF1', 'time': self.__rf1_time}, ignore_index=True)
        self._log.info(f'Execution of refresh function 1 ended successful. Measured time: {self.__rf1_time}')

    def execute_refresh_function2(self):
        return NotImplemented
