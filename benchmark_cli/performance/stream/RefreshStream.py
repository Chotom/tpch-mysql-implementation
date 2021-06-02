import datetime
from typing import Iterator

from benchmark_cli.performance.constants import ORDERS_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST
from benchmark_cli.performance.stream.AbstractStream import AbstractStream
from benchmark_cli.performance.stream.RefreshPair import RefreshPair
from benchmark_cli.performance.utils import data_row_to_query


class RefreshStream(AbstractStream):
    __orders_queries_iter: Iterator[str]
    __lineitem_queries_iter: Iterator[str]

    def __init__(self, logger_name: str, S: int):
        super().__init__(logger_name, False)
        self.__S = S
        self.__rf1_time = datetime.timedelta(0)
        self.__rf2_time = datetime.timedelta(0)

        self.__refresh_pairs = []

        for i in range(S):
            self.__refresh_pairs.append(RefreshPair(logger_name + f'[{i + 1}]', i + 1, self._connection, self._cursor))

    def load_data(self):
        self._log.info('Load queries...')

        for refresh_pair in self.__refresh_pairs:
            refresh_pair.load_data()

        self._log.info('Queries loaded successfully...')

    def execute_stream(self):
        self._log.info('Run refresh stream...')

        for refresh_pair in self.__refresh_pairs:
            refresh_pair.execute_pair()
            # todo: get time of execution
            self._measured_total_time += 0

        self._log.info(f'Execution of refresh stream ended successful. Measured time: {self._measured_total_time}')