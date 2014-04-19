import os
import logging
import importlib

from backdrop.collector import arguments
from backdrop.collector.logging_setup import set_up_logging


def logging_for_entrypoint(entrypoint):
    logfile_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'log')
    set_up_logging(entrypoint, logging.INFO, logfile_path)


def merge_backdrop_config(base, data_set):
    return {
        'url': '{0}/{1}/{2}'.format(
            base['url'],
            data_set['data-group'],
            data_set['data-type']
        ),
        'token': base['token']
    }


def main():
    args = arguments.parse_args('Backdrop Collector')

    entrypoint = args.query['entrypoint']

    # setting up logging will reroute all output to logs/collection.log
    # this can be overridden by passing in --console-logging
    if not args.console_logging:
        logging_for_entrypoint(entrypoint)

    entrypoint_module = importlib.import_module(entrypoint)
    entrypoint_module.main(
        args.config['credentials'],
        merge_backdrop_config(
            args.config['backdrop'],
            args.query['data-set']
        ),
        args.query['query'],
        args.query['options']
    )
