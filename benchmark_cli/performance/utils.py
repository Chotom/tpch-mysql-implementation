"""Utility functions for performance module
"""
import logging
import mysql.connector
from typing import List
from mysql.connector import MySQLConnection

from benchmark_cli.performance.constants import LOG_LEVEL, MYSQL_VALUE_SEP, DB_CONFIG


def data_row_to_query(row: str, table_name: str, quoted_values_indexes: List[int]) -> (int, str):
    """
    Convert string data into mysql insert query.

    :param row: data separated by '|' in string for query to insert
    :param table_name: table name to insert values
    :param quoted_values_indexes: indexes of varchar columns in table
    :return: tuple of record id and Mysql query to execute
    """

    # Remove last '|' or '\n' character and split into values
    values = row.rstrip('\n|').split('|')

    # Surround varchar values with quotes
    for index in quoted_values_indexes:
        values[index] = f"'{values[index]}'"

    return int(values[0]), f'INSERT INTO `{table_name}` VALUES ({MYSQL_VALUE_SEP.join(values)});'


def get_connection(log: logging.Logger, is_buffered: bool):
    """:return: Connection with cursor as tuple"""
    log.info('Trying to connect to database...')
    try:
        connection = MySQLConnection(**DB_CONFIG)
        cursor = connection.cursor(buffered=is_buffered)
    except mysql.connector.Error as e:
        log.error(f'Cannot connect to database: {e}')
        raise
    log.info('Database connected successful.')
    return connection, cursor


def delete_row_to_query(delete_row: str) -> str:
    id = delete_row.rstrip('\n|')
    # return f'DELETE FROM `orders`, `lineitem`' \
    #        f'USING `orders` INNER JOIN `lineitem` ON `orders`.`o_orderkey` = `l_orderkey`' \
    #        f'WHERE O_ORDERKEY = {id};'
    return f'DELETE FROM `lineitem` WHERE l_orderkey = {id}; DELETE FROM `orders` WHERE o_orderkey = {id};'


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
