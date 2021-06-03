import datetime
from typing import Optional, Generator, Any, List

from mysql.connector.cursor import MySQLCursorBuffered, MySQLCursor

from benchmark_cli.performance.constants import QUERY_ORDER, QUERIES_DIR
from benchmark_cli.performance.stream.AbstractStream import AbstractStream


class QueryStream(AbstractStream):

    def __init__(self, logger_name: str, stream_number: int):
        super().__init__(logger_name, True)
        self.__stream_number = stream_number
        self.__query_order: List[int] = QUERY_ORDER[stream_number]
        self.__query_iter: List[Optional[Generator[MySQLCursor, Any, None]]] = []

    def load_data(self):
        self._log.info('Load queries...')

        # Load queries from files to memory
        for i in range(0, 22):
            with open(f'{QUERIES_DIR}/{self.__query_order[i]}.sql') as query_file:
                query = query_file.read()
                self.__query_iter.append(self._cursor.execute(query, multi=True))
        self._log.info('Queries loaded successfully...')

    def execute_stream(self):
        self._log.info('Run query stream...')

        # Execute all queries
        for i in range(0, 22):
            # Measure time for query
            start = datetime.datetime.now()

            # Iterate over generated cursors to execute them and get the results
            cursors = [cur for cur in self.__query_iter[i]]  # for _ in cursors_generator: pass

            time_delta = datetime.datetime.now() - start
            self._measured_total_time += time_delta
            self._df_measures.append({'name': f'Q{self.__query_order[i]}', 'time': time_delta}, ignore_index=True)

            # Print additional information
            self._log.info(f'Time for query {self.__query_order[i]}: {time_delta}')
            for cur in cursors:
                self._log.debug(f'Cursor:\n {cur}')
                if cur.with_rows:
                    self._log.debug(f'Results:\n {cur.fetchall()}')
        self._log.info(f'Execution of query stream ended successful. Measured time: {self._measured_total_time}')
