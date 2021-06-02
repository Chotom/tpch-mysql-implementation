"""Utility functions for performance module
"""
import logging
from typing import List

from benchmark_cli.performance.constants import LOG_LEVEL, MYSQL_VALUE_SEP


def data_row_to_query(row: str, table_name: str, quoted_values_indexes: List[int]) -> (int, str):
    """
    convert string data into mysql insert query

    :param row: data separated by '|' in string for query to insert
    :param table_name: table name to insert values
    :param quoted_values_indexes: indexes of varchar columns in table
    :return: tuple of record id and Mysql query to execute
    """

    # Remove last '|' or '\n' character ans split into values
    values = row.rstrip('\n|').split('|')

    # Surround varchar values with quotes
    for index in quoted_values_indexes:
        values[index] = f"'{values[index]}'"

    return int(values[0]), f'INSERT INTO `{table_name}` VALUES ({MYSQL_VALUE_SEP.join(values)});'


def create_logger(name: str) -> logging.Logger:
    """
    Create logger for given name

    :param name: name of logger
    :return: logger
    """

    log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    if not len(logger.handlers):
        logger.addHandler(console_handler)
    return logger
