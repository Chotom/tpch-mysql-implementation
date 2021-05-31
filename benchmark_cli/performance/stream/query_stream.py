"""Query stream to execute queries in database and measure transaction time
"""

import datetime
from mysql.connector.cursor import MySQLCursorBuffered

from benchmark_cli.performance.constants import QUERY_ORDER, QUERIES_DIR
from benchmark_cli.performance.utils import create_logger


def query_stream(cursor: MySQLCursorBuffered, stream: int) -> datetime.timedelta:
    """
    Execute generated queries in database and measure time.

    :param cursor: Buffered cursor for database
    :param stream: number of stream
    :return:
    """

    # init
    log = create_logger(f'query_stream_{stream}')
    query_stream_time = datetime.timedelta(0)
    query_order = QUERY_ORDER[stream % len(QUERY_ORDER)]

    log.info(f'Running 22 queries in order {query_order}...')
    for i in range(0, 22):
        # Execute query from generated file
        with open(f'{QUERIES_DIR}/{query_order[i]}.sql') as query_file:
            query = query_file.read()
            cursors_generator = cursor.execute(query, multi=True)

            # Measure time for executed query
            start = datetime.datetime.now()
            # for _ in cursors_generator: pass    # iterate over generated cursors to execute them and get the results
            cursors = [cur for cur in cursors_generator]
            measured_time = datetime.datetime.now() - start
            query_stream_time += measured_time

            # Print additional information
            log.info(f'Time for query {query_order[i]}: {measured_time}')
            for cur in cursors:
                log.debug(f'Cursor:\n {cur}')
                if cur.with_rows:
                    log.debug(f'Result:\n {cur.fetchall()}')

    log.info(f'Query stream {stream} ended successful. Measured time: {query_stream_time}')
    return query_stream_time
