from performanceplatform.client.collector import CollectorAPI


def get_config(collector_slug, performanceplatform):
    collector_client = CollectorAPI(
        performanceplatform['stagecraft_url'],
        performanceplatform['omniscient_api_token']
    )
    collector = collector_client.get_collector(collector_slug)
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
