import argparse


def setup(name=""):
    parser = argparse.ArgumentParser(description="%s collector for sending"
                                                 " data to the performance"
                                                 " platform" % name)
    parser.add_argument('-c', '--credentials', dest='credentials',
                        help='JSON file containing credentials '
                             'for the source API')
    parser.add_argument('-q', '--query', dest='query',
                        help='JSON file containing details '
                             'about the query to make'
                             'against the source API '
                             'and the target bucket and'
                             'bearer token for Backdrop')
    args = parser.parse_args()

    if not args.query:
        parser.print_help()
        exit()

    return args
