import logging
# from logging.handlers import RotatingFileHandler


# log_formatter_debug = logging.Formatter(
#     '%(asctime)s\t%(levelname)s\t[%(filename)s:%(lineno)s <> '
#     '%(funcName)s() <> %(threadName)s]\n%(message)s\n'
# )
# handler_debug = RotatingFileHandler('debug.log', mode='a', maxBytes=10000000)
# handler_debug.setLevel(logging.DEBUG)
# handler_debug.setFormatter(log_formatter_debug)

log_formatter_info = logging.Formatter('%(asctime)s\t%(levelname)s\t[%(filename)s]\t%(message)s')
handler_console = logging.StreamHandler()
handler_console.setLevel(logging.INFO)
handler_console.setFormatter(log_formatter_info)


def get_logger(name):
    """
    Usage: logger = get_logger(__name__)
    logger.info('Some log message here')
    :param name: logger name, usually __name__ is fine
    :return: logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # writing a detailed debug log to debug.log file
    # logger.addHandler(handler_debug)

    # writing a general log to console
    logger.addHandler(handler_console)

    return logger
