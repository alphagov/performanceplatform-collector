from performanceplatform.client.collector import CollectorAPI


def get_config(collector_name):
    stagecraft_host = \
        'http://stagecraft.development.performance.service.gov.uk'
    token = 'development-oauth-access-token'

    collector_client = CollectorAPI(stagecraft_host, token)
    collector = collector_client.get_collector(collector_name)
    collector_type = collector_client.get_collector_type(
        collector.get('type').get('slug'))
    return {
        'query': collector.get('query'),
        'options': collector.get('options'),
        'data-set': {
            'data-group': collector.get('data_set').get('data_group'),
            'data-type': collector.get('data_set').get('data_type')
        },
        'entrypoint': collector_type.get('entry_point'),
        'token': collector_type.get('provider').get('slug')
    }
