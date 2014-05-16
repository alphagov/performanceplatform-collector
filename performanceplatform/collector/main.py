import os
import logging
import importlib

from performanceplatform.collector import arguments
from performanceplatform.collector.logging_setup import set_up_logging


def logging_for_entrypoint(entrypoint):
    logfile_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), 'log')
    set_up_logging(entrypoint, logging.INFO, logfile_path)


def merge_performanceplatform_config(
        performanceplatform, data_set, token, dry_run=False):
    return {
        'url': '{0}/{1}/{2}'.format(
            performanceplatform['url'],
            data_set['data-group'],
            data_set['data-type']
        ),
        'token': token['token'],
        'data-group': data_set['data-group'],
        'data-type': data_set['data-type'],
        'dry_run': dry_run
    }


def main():
    args = arguments.parse_args('Performance Platform Collectors')

    entrypoint = args.query['entrypoint']

    if args.console_logging:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
    else:
        logging_for_entrypoint(entrypoint)

    entrypoint_module = importlib.import_module(entrypoint)
    entrypoint_module.main(
        args.credentials,
        merge_performanceplatform_config(
            args.performanceplatform,
            args.query['data-set'],
            args.token,
            args.dry_run
        ),
        args.query['query'],
        args.query['options'],
        args.start_at,
        args.end_at
    )


if __name__ == '__main__':
    main()
