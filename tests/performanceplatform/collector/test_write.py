# -*- coding: utf-8 -*-

from datetime import datetime
import mock
from nose.tools import eq_
from requests import Response, HTTPError
from performanceplatform.collector.write import DataSet
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

    @mock.patch('performanceplatform.collector.write.requests')
    def test_empty_data_set(self, mock_requests):
        data_set = DataSet('some-url', 'some-token')
        data_set.empty_data_set()
        mock_requests.put.assert_called_with(
            url='some-url',
            headers={
                'Authorization': 'Bearer some-token',
                'Content-type': 'application/json'
            },
            data='[]')

    @mock.patch('performanceplatform.collector.write.requests_with_backoff')
    def test_post_data_to_data_set(self, mock_requests_with_backoff):
        data_set = DataSet('foo', 'bar')

        data_set.post({'key': 'value'})

        mock_requests_with_backoff.post.assert_called_with(
            url='foo',
            headers={
                'Authorization': 'Bearer bar',
                'Content-type': 'application/json'
            },
            data='{"key": "value"}'
        )

    @mock.patch('performanceplatform.collector.write.requests_with_backoff')
    def test_post_to_data_set(self, mock_requests_with_backoff):
        data_set = DataSet(None, None)

        data_set.post({'key': datetime(2012, 12, 12)})

        mock_requests_with_backoff.post.assert_called_with(
            url=mock.ANY,
            headers=mock.ANY,
            data='{"key": "2012-12-12T00:00:00+00:00"}'
        )

    @mock.patch('performanceplatform.collector.write.requests_with_backoff')
    def xtest_post_large_data_is_compressed(self, mock_requests_with_backoff):
        data_set = DataSet(None, None)

        big_string = "x" * 3000
        data_set.post({'key': big_string})

        mock_requests_with_backoff.post.assert_called_with(
            url=mock.ANY,
            headers=
            {
                'Content-type': 'application/json',
                'Authorization': 'Bearer None',
                'Content-Encoding': 'gzip',
            },
            data=mock.ANY
        )

        call_args = mock_requests_with_backoff.post.call_args

        gzipped_bytes = call_args[1]["data"].getvalue()

        # large repeated string compresses really well – who knew?
        self.assertEqual(52, len(gzipped_bytes))

        # Does it look like a gzipped stream of bytes?
        # http://tools.ietf.org/html/rfc1952#page-5
        self.assertEqual(b'\x1f', gzipped_bytes[0])
        self.assertEqual(b'\x8b', gzipped_bytes[1])
        self.assertEqual(b'\x08', gzipped_bytes[2])

    @mock.patch('performanceplatform.utils.requests_with_backoff.post')
    def test_raises_error_on_4XX_or_5XX_responses(self, mock_post):
        data_set = DataSet(None, None)
        response = Response()
        response.status_code = 418
        mock_post.return_value = response
        self.assertRaises(HTTPError, data_set.post, {'key': 'foo'})

    @mock.patch('performanceplatform.collector.write.requests_with_backoff')
    @mock.patch('performanceplatform.collector.write.logging')
    def test_logs_on_dry_run(self, mock_logging, mock_requests_with_backoff):
        data_set = DataSet(None, None, dry_run=True)
        data_set.post({'key': datetime(2012, 12, 12)})

        eq_(mock_logging.info.call_count, 1)
        eq_(mock_requests_with_backoff.post.call_count, 0)
