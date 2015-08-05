import argparse
import unittest
from hamcrest import assert_that, equal_to
from mock import patch
from performanceplatform.collector.arguments import parse_args
from performanceplatform.utils.collector import get_config
from tests.performanceplatform.collector.tools import json_file


class ConfigTest(unittest.TestCase):
    @patch('performanceplatform.client.collector.CollectorAPI.get_collector')
    @patch(
        'performanceplatform.client.collector.CollectorAPI.get_collector_type')
    def test_get_config(self, mock_collector_type, mock_collector):

        mock_collector.return_value = {
            'id': '1234',
            'name': 'foo',
            'slug': 'foo',
            'query': {},
            'options': {},
            'type': {
                'id': '1234',
                'name': 'foo-type'
            },
            'data_source': {
                'id': '1234',
                'slug': 'foo-data-source'
            },
            'data_set': {
                'data_type': 'foo-data-type',
                'data_group': 'foo-data-group'
            }
        }

        mock_collector_type.return_value = {
            'id': '1234',
            'name': 'foo-type',
            'entry_point': 'foo.collector',
            'query_schema': {},
            'options_schema': {},
            'provider': {
                'id': '1234',
                'slug': 'foo-provider'
            }
        }

        config_data = get_config('foo')
        expected_data = {
            'query': {},
            'options': {},
            'data-set': {
                'data-group': 'foo-data-group',
                'data-type': 'foo-data-type'
            },
            "entrypoint": 'foo.collector',
            "token": "foo-provider"
        }

        assert_that(config_data, equal_to(expected_data))
