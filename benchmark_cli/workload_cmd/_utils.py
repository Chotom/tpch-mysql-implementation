import logging

# Consts

LOG_LEVEL = 'INFO'


# Functions

# def generate_queries()
# def generate_rf()

def create_logger(name: str) -> logging.Logger:
    """
    Create logger for given name

    :param name: name of logger
    :return: logger
    """
    log_format = '%(asctime)s - %(name)s - %(levelname)s: %(message)s'
    formatter = logging.Formatter(log_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)
    if not len(logger.handlers):
        logger.addHandler(console_handler)
    return logger
