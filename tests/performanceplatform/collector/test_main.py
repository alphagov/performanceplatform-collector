from argparse import Namespace
from hamcrest import assert_that, equal_to
import mock
import os
import unittest

from performanceplatform.collector import main


class TestMain(unittest.TestCase):

    def test_merge_performanceplatform_config(self):
        performanceplatform = {
            'url': 'http://foo/data',
        }
        data_set = {
            'data-group': 'group',
            'data-type': 'type'
        }
        token = {
            'token': 'foo',
        }

        merged = main.merge_performanceplatform_config(
            performanceplatform, data_set, token)

        assert_that(merged['url'], equal_to('http://foo/data/group/type'))
        assert_that(merged['dry_run'], equal_to(False))

    def test_merge_performanceplatform_config_with_dry_run(self):
        performanceplatform = {
            'url': 'http://foo/data',
        }
        data_set = {
            'data-group': 'group',
            'data-type': 'type'
        }
        token = {
            'token': 'foo',
        }

        merged = main.merge_performanceplatform_config(
            performanceplatform, data_set, token, dry_run=True)

        assert_that(merged['url'], equal_to('http://foo/data/group/type'))
        assert_that(merged['dry_run'], equal_to(True))

    @mock.patch('performanceplatform.collector.main.'
                '_log_collector_instead_of_running')
    @mock.patch('performanceplatform.collector.main._run_collector')
    @mock.patch('performanceplatform.collector.arguments.parse_args')
    @mock.patch(
        'performanceplatform.client.collector.CollectorAPI.get_collector')
    def test_collectors_can_be_disabled(self,
                                        mock_get_collector,
                                        mock_parse_args,
                                        mock_run_collector,
                                        mock_log_collector_instead_of_running):
        orig_disable_collectors = os.getenv('DISABLE_COLLECTORS')
        try:
            os.environ['DISABLE_COLLECTORS'] = 'false'
            main.main()
            assert mock_run_collector.called
            assert not mock_log_collector_instead_of_running.called

            mock_run_collector.reset_mock()
            mock_log_collector_instead_of_running.reset_mock()

            os.environ['DISABLE_COLLECTORS'] = 'true'
            main.main()
            assert not mock_run_collector.called
            assert mock_log_collector_instead_of_running.called
        finally:
            if orig_disable_collectors:
                os.environ['DISABLE_COLLECTORS'] = orig_disable_collectors
            else:
                os.unsetenv('DISABLE_COLLECTORS')

    @mock.patch('performanceplatform.collector.main._run_collector')
    @mock.patch('performanceplatform.utils.collector.get_config')
    @mock.patch('performanceplatform.collector.arguments.parse_args')
    def test_get_config_not_called_if_query_file_in_args(self,
                                                         mock_parse_args,
                                                         mock_get_config,
                                                         mock_run_collector):
        mock_parse_args.return_value = Namespace(query={})
        try:
            main.main()
            assert not mock_get_config.called
        except:
            pass

    @mock.patch('performanceplatform.collector.main._run_collector')
    @mock.patch(
        'performanceplatform.client.collector.CollectorAPI.get_collector')
    @mock.patch('performanceplatform.utils.collector.get_config')
    @mock.patch('performanceplatform.collector.arguments.parse_args')
    def test_get_config_called_if_collector_in_args(self,
                                                    mock_parse_args,
                                                    mock_get_config,
                                                    mock_get_collector,
                                                    mock_run_collector):
        mock_parse_args.return_value = Namespace(
            collector_slug="some-collector")
        try:
            main.main()
            assert mock_get_config.called
        except:
            pass
