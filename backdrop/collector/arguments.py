import argparse
import json
from dateutil.parser import parse as parse_date


def parse_args(name="", args=None):
    """Parse command line argument for a collector

    Returns an argparse.Namespace with 'credentials' and 'query' options"""
    def _load_json_file(path):
        with open(path) as f:
            return json.load(f)

    parser = argparse.ArgumentParser(description="%s collector for sending"
                                                 " data to the performance"
                                                 " platform" % name)
    parser.add_argument('-c', '--credentials', dest='credentials',
                        type=_load_json_file,
                        help='JSON file containing credentials '
                             'for the source API',
                        required=True)
    parser.add_argument('-q', '--query', dest='query',
                        type=_load_json_file,
                        help='JSON file containing details '
                             'about the query to make'
                             'against the source API '
                             'and the target bucket and'
                             'bearer token for Backdrop',
                        required=True)
    parser.add_argument('-s', '--start', dest='start_at',
                        type=parse_date,
                        help='Date to start collection from')
    parser.add_argument('-e', '--end', dest='end_at',
                        type=parse_date,
                        help='Date to end collection')
    args = parser.parse_args(args)

    return args
