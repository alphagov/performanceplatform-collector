from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

import logging
import sys

from performanceplatform.collector.ga.core \
    import create_client, query_documents_for, send_data

from performanceplatform.collector.write import DataSet


def main(credentials, data_set_config, query, options, start_at, end_at):
    if credentials is None:
        logging.error('{0} requires credentials, see the README'.format(
            __name__))
        sys.exit()

    client = create_client(credentials)

    documents = query_documents_for(
        client, query, options,
        data_set_config['data-type'], start_at, end_at
    )

    data_set = DataSet.from_config(data_set_config)
    send_data(data_set, documents)
