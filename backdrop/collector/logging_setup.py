from logstash_formatter import LogstashFormatter
import logging
import os
import pdb


def get_log_file_handler(path):
    handler = logging.FileHandler(path)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s [%(levelname)s] -> %(message)s"))
    return handler


def get_json_log_handler(path, app_name):
    handler = logging.FileHandler(path)
    formatter = LogstashFormatter()
    formatter.defaults['@tags'] = ['collector', app_name]
    handler.setFormatter(formatter)
    return handler


def set_up_logging(app_name, log_level, logfile_path):
    logger = logging.getLogger()
    logger.setLevel(log_level)
    logger.addHandler(get_log_file_handler(
        os.path.join(logfile_path, 'collector.log')))
    logger.addHandler(get_json_log_handler(
        os.path.join(logfile_path, 'collector.log.json'), app_name))
    logger.info("{0} logging started".format(app_name))
