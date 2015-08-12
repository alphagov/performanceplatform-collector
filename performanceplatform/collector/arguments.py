import argparse
import json
from dateutil.parser import parse as parse_date


def parse_args(name="", args=None):
    """Parse command line argument for a collector

    Returns an argparse.Namespace with 'config' and 'query' options"""
    def _load_json_file(path):
        with open(path) as f:
            json_data = json.load(f)
            json_data['path_to_json_file'] = path
            return json_data

    parser = argparse.ArgumentParser(description="%s collector for sending"
                                                 " data to the performance"
                                                 " platform" % name)
    parser.add_argument('-c', '--credentials', dest='credentials',
                        type=_load_json_file,
                        help='JSON file containing credentials '
                             'for the collector',
                        required=True)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-l', '--collector', dest='collector_slug',
                       type=str,
                       help='Collector slug to query the API for the '
                            'collector config')
    group.add_argument('-q', '--query', dest='query',
                       type=_load_json_file,
                       help='JSON file containing details '
                            'about the query to make '
                            'against the source API '
                            'and the target data-set')
    parser.add_argument('-t', '--token', dest='token',
                        type=_load_json_file,
                        help='JSON file containing token '
                             'for the collector',
                        required=True)
    parser.add_argument('-b', '--performanceplatform',
                        dest='performanceplatform',
                        type=_load_json_file,
                        help='JSON file containing the Performance Platform '
                             'config for the collector',
                        required=True)
    parser.add_argument('-s', '--start', dest='start_at',
                        type=parse_date,
                        help='Date to start collection from')
    parser.add_argument('-e', '--end', dest='end_at',
                        type=parse_date,
                        help='Date to end collection')
    parser.add_argument('--console-logging', dest='console_logging',
                        action='store_true',
                        help='Output logging to the console rather than file')
    parser.add_argument('--dry-run', dest='dry_run',
                        action='store_true',
                        help='Instead of pushing to the Performance Platform '
                             'the collector will print out what would have '
                             'been pushed')
    parser.set_defaults(console_logging=False, dry_run=False)
    args = parser.parse_args(args)

    return args
