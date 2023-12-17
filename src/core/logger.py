import logging

LOGGER_NAME = 'tr'


def get_tr_logger():
    return logging.getLogger(LOGGER_NAME)


tr_logger = get_tr_logger()
