import os
import logging
import importlib

from collections import OrderedDict

from performanceplatform.collector import arguments
from performanceplatform.collector.logging_setup import (
    set_up_logging, close_down_logging)
from performanceplatform.utils.collector import get_config


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


def _get_path_to_json_file(query):
    return query['path_to_json_file']


def make_extra_json_fields(args):
    """
    From the parsed command-line arguments, generate a dictionary of additional
    fields to be inserted into JSON logs (logstash_formatter module)
    """
    extra_json_fields = {
        'data_group': _get_data_group(args.query),
        'data_type': _get_data_type(args.query),
        'data_group_data_type': _get_data_group_data_type(args.query),
        'query': _get_query_params(args.query),
    }
    if "path_to_json_file" in args.query:
        extra_json_fields['path_to_query'] = _get_path_to_json_file(args.query)
    return extra_json_fields


def logging_for_entrypoint(
        entrypoint, json_fields, logfile_path, logfile_name):
    if logfile_path is None:
        logfile_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '..', '..', 'log')
    loglevel = getattr(logging, os.environ.get('LOGLEVEL', 'INFO').upper())
    set_up_logging(
        entrypoint, loglevel, logfile_path, logfile_name, json_fields)


def _log_collector_instead_of_running(entrypoint, args):
    logged_args = {
        'start_at': args.start_at,
        'end_at': args.end_at,
        'query': {k: args.query[k] for k in ('data-set', 'query', 'options')}
    }
    logging.info(
        'Collector {} NOT run with the following {}'.format(entrypoint,
                                                            logged_args))


def merge_performanceplatform_config(
        performanceplatform, data_set, token, dry_run=False):
    return {
        'url': '{0}/{1}/{2}'.format(
            performanceplatform['backdrop_url'],
            data_set['data-group'],
            data_set['data-type']
        ),
        'token': token['token'],
        'data-group': data_set['data-group'],
        'data-type': data_set['data-type'],
        'dry_run': dry_run
    }


def _run_collector(entrypoint, args, logfile_path=None, logfile_name=None):
    if args.console_logging:
        logging.basicConfig(level=logging.INFO)
    else:
        logging_for_entrypoint(
            entrypoint,
            make_extra_json_fields(args),
            logfile_path,
            logfile_name
        )

    if os.environ.get('DISABLE_COLLECTORS', 'false') == 'true':
        _log_collector_instead_of_running(entrypoint, args)
    else:
        entrypoint_module = importlib.import_module(entrypoint)
        logging.info('Running collection into {}/{}'.format(
            args.query.get('data-set')['data-group'],
            args.query.get('data-set')['data-type']))
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
        if not args.console_logging:
            close_down_logging()


def main():
    args = arguments.parse_args('Performance Platform Collector')
    if args.collector_slug:
        args.query = get_config(args.collector_slug, args.performanceplatform)

    _run_collector(args.query['entrypoint'], args)


if __name__ == '__main__':
    main()
