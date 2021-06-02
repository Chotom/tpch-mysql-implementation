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
        self.__log = create_logger(logger_name)
        self.__data_path = data_path
        self.__measured_total_time = datetime.timedelta(0)
        self.__df_measures = pd.DataFrame({'name': str, 'time': np.timedelta64}).set_index('name')

        # Set session
        try:
            self.__log.info('Trying to connect to database...')
            self.__connection = MySQLConnection(**DB_CONFIG)
            self.__cursor = self.__connection.cursor(buffered=is_buffered)
        except mysql.connector.Error as e:
            self.__log.error(f'Cannot connect to database: {e}')
            raise
        self.__log.info('Database connected successful.')

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
        self.__df_measures.append({'name': f'total_time', 'time': self.__measured_total_time})
        return self.__df_measures

    def __del__(self):
        self.__connection.close()
        self.__cursor.close()
