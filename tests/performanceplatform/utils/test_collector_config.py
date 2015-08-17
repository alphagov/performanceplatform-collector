import unittest
from hamcrest import assert_that, equal_to
from mock import patch
from performanceplatform.utils.collector import get_config


class ConfigTest(unittest.TestCase):
    @patch('performanceplatform.client.collector.CollectorAPI.get_collector')
    def test_get_config(self, mock_collector):

        mock_collector.return_value = {
            'id': '1234',
            'name': 'foo',
            'slug': 'foo',
            'query': {},
            'options': {},
            'entry_point': 'foo.collector',
            'type': {
                'id': '1234',
                'slug': 'foo-type',
                'name': 'foo-type'
            },
            'data_source': {
                'id': '1234',
                'slug': 'foo-data-source'
            },
            'data_set': {
                'data_type': 'foo-data-type',
                'data_group': 'foo-data-group',
                'bearer_token': 'foo-token'
            },
            'provider': {
                'id': '1234',
                'slug': 'foo-provider'
            }
        }

        performanceplatform = {
            'url': 'http://foo/data',
            'stagecraft_url': 'http://foo.config.uk',
            'omniscient_api_token': 'fake-access-token'
        }

        config_data = get_config('foo', performanceplatform)
        expected_data = {
            'query': {},
            'options': {},
            'data-set': {
                'data-group': 'foo-data-group',
                'data-type': 'foo-data-type',
                'bearer_token': 'foo-token'
            },
            "entrypoint": 'foo.collector',
            "token": "foo-provider"
        }

        assert_that(config_data, equal_to(expected_data))
