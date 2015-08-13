from performanceplatform.client.collector import CollectorAPI


def get_config(collector_name):
    stagecraft_host = \
        'http://stagecraft.development.performance.service.gov.uk'
    token = 'development-oauth-access-token'

    collector_client = CollectorAPI(stagecraft_host, token)
    collector = collector_client.get_collector(collector_name)
    return {
        'query': collector.get('query'),
        'options': collector.get('options'),
        'data-set': {
            'data-group': collector.get('data_set').get('data_group'),
            'data-type': collector.get('data_set').get('data_type'),
            'bearer_token': collector.get('data_set').get('bearer_token')
        },
        'entrypoint': collector.get('entry_point'),
        'token': collector.get('provider').get('slug')
    }
