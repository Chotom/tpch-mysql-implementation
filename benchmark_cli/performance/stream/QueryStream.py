import datetime
from typing import Optional, Generator, Any, List

from mysql.connector.cursor import MySQLCursorBuffered, MySQLCursor

from benchmark_cli.performance.constants import QUERY_ORDER
from benchmark_cli.performance.stream.AbstractStream import AbstractStream


class QueryStream(AbstractStream):
    __query_iter: Optional[Generator[MySQLCursor, Any, None]]

    def __init__(self, logger_name: str, data_path: str, is_buffered: bool, stream_number: int):
        super().__init__(logger_name, data_path, is_buffered)
        self.__stream_number = stream_number
        self.__query_order: List[int] = QUERY_ORDER[stream_number]

    def load_data(self):
        self.__log.info('Load queries...')

        # Load queries from files to memory
        for i in range(0, 22):
            with open(f'{self.__data_path}/{self.__query_order[i]}.sql') as query_file:
                query = query_file.read()
                self.__query_iter = self.__cursor.execute(query, multi=True)
        self.__log.info('Queries loaded successfully...')

    def execute_stream(self):
        self.__log.info('Run query stream...')

        # Execute all queries
        for i in range(0, 22):
            # Measure time for query
            start = datetime.datetime.now()

            # Iterate over generated cursors to execute them and get the results
            cursors = [cur for cur in self.__query_iter]  # for _ in cursors_generator: pass

            time_delta = datetime.datetime.now() - start
            self.__measured_total_time += time_delta
            self.__df_measures.append({'name': f'Q{self.__query_order[i]}', 'time': time_delta})

            # Print additional information
            self.__log.info(f'Time for query {self.__query_order[i]}: {time_delta}')
            for cur in cursors:
                self.__log.debug(f'Cursor:\n {cur}')
                if cur.with_rows:
                    self.__log.debug(f'Results:\n {cur.fetchall()}')
        self.__log.info(f'Execution of query stream ended successful. Measured time: {self.__measured_total_time}')
