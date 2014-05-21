from logstash_formatter import LogstashFormatter
import logging
import os
import pdb
import sys
import traceback


def get_log_file_handler(path):
    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] -> %(message)s"))
    return handler


def get_json_log_handler(path, app_name, json_fields):
    handler = logging.FileHandler(path)
    formatter = LogstashFormatter()
    formatter.defaults['@tags'] = ['collector', app_name]
    formatter.defaults['@fields'] = json_fields
    handler.setFormatter(formatter)
    return handler


def uncaught_exception_handler(*exc_info):
    text = "".join(traceback.format_exception(*exc_info))
    logging.error("Unhandled exception: %s", text)


def set_up_logging(app_name, log_level, logfile_path, json_fields=None):
    sys.excepthook = uncaught_exception_handler
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(get_log_file_handler(
        os.path.join(logfile_path, 'collector.log')))
    logger.addHandler(get_json_log_handler(
        os.path.join(logfile_path, 'collector.log.json'),
        app_name,
        json_fields=json_fields if json_fields else {}))
    logger.info("{0} logging started".format(app_name))
