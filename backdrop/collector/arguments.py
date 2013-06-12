import argparse
import json


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
                             'for the source API')
    parser.add_argument('-q', '--query', dest='query',
                        type=_load_json_file,
                        help='JSON file containing details '
                             'about the query to make'
                             'against the source API '
                             'and the target bucket and'
                             'bearer token for Backdrop')
    args = parser.parse_args(args)

    if args.query is None:
        parser.print_help()
        exit()

    return args
