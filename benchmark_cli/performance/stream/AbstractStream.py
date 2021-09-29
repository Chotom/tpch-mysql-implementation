import datetime
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from benchmark_cli.performance.constants import DB_CONFIG
from benchmark_cli.performance.utils import create_logger, get_connection


class AbstractStream(ABC):

    def __init__(self, logger_name: str, is_buffered: bool, conn: MySQLConnection = None, cursor: MySQLCursor = None):
        self._log = create_logger(logger_name)
        self._df_measures = pd.DataFrame(columns=['name', 'time'], dtype=np.dtype(str, np.timedelta64)).set_index('name')
        self.__set_session__(is_buffered, conn, cursor)

    @abstractmethod
    def load_data(self):
        """Prepare data and queries to be executed in database."""
        return NotImplemented

    @abstractmethod
    def execute_stream(self):
        """Execute queries in database and measure performance."""
        return NotImplemented

    @property
    def df_measures(self) -> pd.DataFrame:
        """:return: dataframe with measured time execution of queries."""
        return self._df_measures.set_index('name')

    def __set_session__(self, is_buffered: bool, conn: MySQLConnection, cursor: MySQLCursor):
        if conn is not None and cursor is not None:
            self._connection, self._cursor = conn, cursor
        elif conn is None and cursor is None:
            self._connection,  self._cursor = get_connection(self._log, is_buffered)
        else:
            self._log.error('Database connection or cursor is not defined. Error creating stream object: missing arg.')
            raise

    def __del__(self):
        self._cursor.reset()
        self._connection.close()
