import os
import logging
import importlib

from collections import OrderedDict

from performanceplatform.collector import arguments
from performanceplatform.collector.logging_setup import set_up_logging


def logging_for_entrypoint(entrypoint, json_fields):
    logfile_path = os.path.join(
        os.path.dirname(os.path.realpath(__file__)), '..', '..', 'log')
    loglevel = getattr(logging, os.environ.get('LOGLEVEL', 'INFO').upper())
    set_up_logging(entrypoint, loglevel, logfile_path, json_fields)


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


def make_extra_json_fields(args):
    """
    From the parsed command-line arguments, generate a dictionary of additional
    fields to be inserted into JSON logs (logstash_formatter module)
    """
    return {
        'data_group': _get_data_group(args.query),
        'data_type': _get_data_type(args.query),
        'data_group_data_type': _get_data_group_data_type(args.query),
        'query': _get_query_params(args.query),
    }


def _get_data_group(query):
    return query['data-set']['data-group']


def _get_data_type(query):
    return query['data-set']['data-type']


def _get_data_group_data_type(query):
    return '{}/{}'.format(_get_data_group(query), _get_data_type(query))


def _get_query_params(query):
    """
    >>> _get_query_params({'query': {'a': 1, 'c': 3, 'b': 5}})
    'a=1 b=5 c=3'
    """
    query_params = OrderedDict(sorted(query['query'].items()))
    return ' '.join(['{}={}'.format(k, v) for k, v in query_params.items()])


def main():
    args = arguments.parse_args('Performance Platform Collector')

    entrypoint = args.query['entrypoint']

    if args.console_logging:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
    else:
        logging_for_entrypoint(entrypoint, make_extra_json_fields(args))

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
