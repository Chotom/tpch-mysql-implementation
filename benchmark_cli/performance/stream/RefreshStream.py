import datetime
import random
from typing import Iterator

from benchmark_cli.performance.constants import ORDER_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST
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
        self.__log.info('Load queries...')

        # Load queries from files to memory
        with open(f'{self.__data_path}/orders.tbl.u{self.__run_number}', 'r') as orders_file:
            orders_queries = orders_file.readlines()
            for i, values_row in enumerate(orders_queries):
                orders_queries[i] = data_row_to_query(values_row, 'order', ORDER_QUOTE_INDEX_LIST)
            self.__orders_queries_iter = iter(orders_queries)

        with open(f'{self.__data_path}/lineitem.tbl.u{self.__run_number}', 'r') as lineitem_file:
            lineitem_queries = lineitem_file.readlines()
            for i, values_row in enumerate(lineitem_queries):
                lineitem_queries[i] = data_row_to_query(values_row, 'lineitem', LINEITEM_QUOTE_INDEX_LIST)
            self.__lineitem_queries_iter = iter(lineitem_queries)
        self.__log.info('Queries loaded successfully...')

    def execute_stream(self):
        self.__log.info('Run refresh stream...')

        self.execute_refresh_function1()
        self.execute_refresh_function2()

        self.__log.info(f'Execution of refresh stream ended successful. Measured time: {self.__measured_total_time}')

    def execute_refresh_function1(self):
        self.__log.info('Run refresh function 1...')

        # Execute insert queries
        for i in range(int(self.__scale * 1500)):
            order_query = next(self.__orders_queries_iter)

            # Measure time for transaction
            start = datetime.datetime.now()

            # Insert new row into `orders` table
            self.__cursor.execute(f'{order_query}')
            for j in range(random.randint(1, 7)):
                # Insert new row into `lineitem` table
                self.__cursor.execute(f'{next(self.__lineitem_queries_iter)}')
            self.__connection.commit()

            time_delta = datetime.datetime.now() - start
            self.__rf1_time += time_delta

            # Print additional information
            self.__log.debug(f'Time for {order_query} query: {time_delta}')
        self.__df_measures.append({'name': f'RF1', 'time': self.__rf1_time})
        self.__log.info(f'Execution of refresh function 1 ended successful. Measured time: {self.__rf1_time}')

    def execute_refresh_function2(self):
        return NotImplemented
