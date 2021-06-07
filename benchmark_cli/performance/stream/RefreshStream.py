import datetime
from typing import Iterator

from benchmark_cli.performance.constants import ORDERS_QUOTE_INDEX_LIST, LINEITEM_QUOTE_INDEX_LIST
from benchmark_cli.performance.stream.AbstractStream import AbstractStream
from benchmark_cli.performance.stream.RefreshPair import RefreshPair
from benchmark_cli.performance.utils import data_row_to_query


class RefreshStream(AbstractStream):
    __orders_queries_iter: Iterator[str]
    __lineitem_queries_iter: Iterator[str]

    def __init__(self, logger_name: str, stream_count: int, refresh_file_start_index: int):
        super().__init__(logger_name, False)
        self.__S = stream_count
        self.__rf1_time = datetime.timedelta(0)
        self.__rf2_time = datetime.timedelta(0)
        self.__file_start_index = refresh_file_start_index

        self.__refresh_pairs = []

        for i in range(stream_count):
            self.__refresh_pairs.append(RefreshPair(f'refresh_pair_{i + 1}', i + self.__file_start_index , self._connection, self._cursor))

    def load_data(self):
        self._log.info('Load queries...')

        for refresh_pair in self.__refresh_pairs:
            refresh_pair.load_data()

        self._log.info('Queries loaded successfully...')

    def execute_stream(self):
        self._log.info('Run refresh stream...')

        for i, refresh_pair in enumerate(self.__refresh_pairs):
            refresh_pair.execute_pair()
            rpair_time = refresh_pair.df_measures.at['total_time', 'time']
            self._df_measures = self._df_measures.append({'name': f'RF1_{i}', 'time': refresh_pair.df_measures.at[f'RF1_{i + self.__file_start_index }', 'time']}, ignore_index=True)
            self._df_measures = self._df_measures.append({'name': f'RF2_{i}', 'time': refresh_pair.df_measures.at[f'RF2_{i + self.__file_start_index }', 'time']}, ignore_index=True)
            self._measured_total_time += rpair_time

        self._log.info(f'Execution of refresh stream ended successful. Measured time: {self._measured_total_time}')
