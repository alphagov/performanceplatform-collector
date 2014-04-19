from datetime import datetime
import mock
from nose.tools import eq_
from requests import Response, HTTPError
from backdrop.collector.write import DataSet
import unittest


class TestDataSet(unittest.TestCase):
    def test_from_target(self):
        data_set = DataSet(url='foo', token='bar')
        eq_(data_set.url, 'foo')
        eq_(data_set.token, 'bar')
        eq_(data_set.dry_run, False)

    def test_from_config(self):
        data_set = DataSet.from_config({
            'url': 'foo',
            'token': 'bar',
            'dry_run': True,
        })
        eq_(data_set.url, 'foo')
        eq_(data_set.token, 'bar')
        eq_(data_set.dry_run, True)

    @mock.patch('backdrop.collector.write.requests')
    def test_post_data_to_data_set(self, requests):
        data_set = DataSet('foo', 'bar')

        data_set.post({'key': 'value'})

        requests.post.assert_called_with(
            url='foo',
            headers={
                'Authorization': 'Bearer bar',
                'Content-type': 'application/json'
            },
            data='{"key": "value"}'
        )

    @mock.patch('backdrop.collector.write.requests')
    def test_post_to_data_set(self, requests):
        data_set = DataSet(None, None)

        data_set.post({'key': datetime(2012, 12, 12)})

        requests.post.assert_called_with(
            url=mock.ANY,
            headers=mock.ANY,
            data='{"key": "2012-12-12T00:00:00+00:00"}'
        )

    @mock.patch('requests.post')
    def test_raises_error_on_4XX_or_5XX_responses(self, mock_post):
        data_set = DataSet(None, None)
        response = Response()
        response.status_code = 418
        mock_post.return_value = response
        self.assertRaises(HTTPError, data_set.post, {'key': 'foo'})

    @mock.patch('backdrop.collector.write.requests')
    @mock.patch('backdrop.collector.write.logging')
    def test_logs_on_dry_run(self, mock_logging, mock_requests):
        data_set = DataSet(None, None, dry_run=True)
        data_set.post({'key': datetime(2012, 12, 12)})

        eq_(mock_logging.info.call_count, 3)
        eq_(mock_requests.post.call_count, 0)
