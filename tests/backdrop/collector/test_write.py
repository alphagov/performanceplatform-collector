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

    def test_from_config(self):
        data_set = DataSet.from_config({
            'url': 'foo',
            'token': 'bar'
        })
        eq_(data_set.url, 'foo')
        eq_(data_set.token, 'bar')

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
