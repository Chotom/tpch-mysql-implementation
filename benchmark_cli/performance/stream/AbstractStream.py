import datetime
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from benchmark_cli.performance.constants import DB_CONFIG
from benchmark_cli.performance.utils import create_logger


class AbstractStream(ABC):

    def __init__(self, logger_name: str, is_buffered: bool, conn: MySQLConnection = None, cursor: MySQLCursor = None):
        # Init
        self._log = create_logger(logger_name)
        self._measured_total_time = datetime.timedelta(0)
        self._df_measures = pd.DataFrame(columns=['name', 'time'], dtype=np.dtype(str, np.timedelta64)).set_index('name')

        # Set session
        self._log.info('Trying to connect to database...')
        if conn is not None and cursor is not None:
            self._connection = conn
            self._cursor = cursor
        elif conn is None and cursor is None:
            try:
                self._connection = MySQLConnection(**DB_CONFIG)
                self._cursor = self._connection.cursor(buffered=is_buffered)
            except mysql.connector.Error as e:
                self._log.error(f'Cannot connect to database: {e}')
                raise
        else:
            self._log.error('Database connection or cursor is not defined. Error creating stream object: missing arg.')
            raise
        self._log.info('Database connected successful.')

    @abstractmethod
    def load_data(self):
        """
        Prepare data and queries to be executed in database
        """
        return NotImplemented

    @abstractmethod
    def execute_stream(self):
        """
        Execute queries in database and measure performance
        """
        return NotImplemented

    @property
    def df_measures(self) -> pd.DataFrame:
        """
        :return: dataframe with measured queries and total_time of execution
        """
        return self._df_measures.append({'name': 'total_time', 'time': self._measured_total_time}, ignore_index=True).set_index('name')

    def __del__(self):
        self._cursor.reset()
        self._connection.close()
