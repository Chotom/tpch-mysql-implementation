import datetime
from abc import ABC, abstractmethod
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import DB_CONFIG
from benchmark_cli.performance.utils import create_logger


class AbstractStream(ABC):
    def __init__(self, logger_name: str, data_path: str, is_buffered: bool):
        # Init
        self._log = create_logger(logger_name)
        self._data_path = data_path

        # Set session
        try:
            self._log.info('Trying to connect to database...')
            self._connection = MySQLConnection(**DB_CONFIG)
            self._cursor = self._connection.cursor(buffered=is_buffered)
        except mysql.connector.Error as e:
            self._log.error(f'Cannot connect to database: {e}')
            raise
        self._log.info('Database connected successful.')

        self._measured_total_time = datetime.timedelta(0)
        self._df_measures = pd.DataFrame(columns=['name', 'time'], dtype=np.dtype(str, np.timedelta64)).set_index('name')

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
        self._df_measures.append({'name': f'total_time', 'time': self._measured_total_time})
        return self._df_measures

    def __del__(self):
        self._cursor.close()
        self._connection.close()
