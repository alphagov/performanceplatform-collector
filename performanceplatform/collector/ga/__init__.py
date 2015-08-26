from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

from performanceplatform.collector.ga.core \
    import create_client, query_documents_for

from performanceplatform.utils.data_pusher import Pusher


def main(credentials, data_set_config, query, options, start_at, end_at):
    client = create_client(credentials)

    documents = query_documents_for(
        client, query, options,
        data_set_config['data-type'],
        start_at, end_at)

    if query.get('empty_data_set'):
        options['empty_data_set'] = True

    Pusher(data_set_config, options).push(documents)
