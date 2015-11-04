from logstash_formatter import LogstashFormatter
import logging
from cloghandler import ConcurrentRotatingFileHandler
import os
import sys
import traceback


def get_log_file_handler(path):
    handler = ConcurrentRotatingFileHandler(
        path, "a", 2 * 1024 * 1024 * 1024, 1)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] -> %(message)s"))
    return handler


def get_json_log_handler(path, app_name, json_fields):
    handler = ConcurrentRotatingFileHandler(
        path, "a", 2 * 1024 * 1024 * 1024, 1)
    formatter = LogstashFormatter()
    formatter.defaults['@tags'] = ['collector', app_name]
    formatter.defaults['@fields'] = json_fields
    handler.setFormatter(formatter)
    return handler


def uncaught_exception_handler(*exc_info):
    text = "".join(traceback.format_exception(*exc_info))
    (exc_class, exc_message, _) = exc_info
    extra = {
        'exception_class': exc_class.__name__,
        'exception_message': exc_message
    }
    logging.error("Unhandled exception: %s", text, extra=extra)


def set_up_logging(
        app_name,
        log_level,
        logfile_path,
        logfile_name,
        json_fields=None):

    if logfile_name is None:
        logfile_name = 'production'
    sys.excepthook = uncaught_exception_handler
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(get_log_file_handler(
        os.path.join(logfile_path, '{}.log'.format(logfile_name))))
    logger.addHandler(get_json_log_handler(
        os.path.join(logfile_path, '{}.json.log'.format(logfile_name)),
        app_name,
        json_fields=json_fields if json_fields else {}))
    logger.info("{0} logging started".format(app_name))


def close_down_logging():
    logger = logging.getLogger()
    handlers = logger.handlers[:]
    for handler in handlers:
        handler.close()
        logger.removeHandler(handler)


def extra_fields_from_exception(exception):
    """Exception into a dict to be passed to logger

    Standardise the way we send exception messages so they can be filtered in
    kibana, also make it easy to add extra information in future.
    """
    return {
        'exception_class': type(exception).__name__,
        'exception_message': exception.message
    }
