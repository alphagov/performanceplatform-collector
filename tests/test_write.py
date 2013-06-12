from datetime import datetime
import mock
from nose.tools import *
from backdrop.collector.write import Bucket


class TestBucket(object):
    def test_from_target(self):
        bucket = Bucket.from_target({
            'url': 'foo',
            'token': 'bar'
        })
        eq_(bucket.url, 'foo')
        eq_(bucket.token, 'bar')

    def test_from_target_fails_with_invalid_dict(self):
        assert_raises(KeyError, Bucket.from_target, {
            'foo': 'bar'
        })

    @mock.patch('backdrop.collector.write.requests')
    def test_post_data_to_bucket(self, requests):
        bucket = Bucket('foo', 'bar')

        bucket.post({'key': 'value'})

        requests.post.assert_called_with(
            url='foo',
            headers={
                'Authorization': 'Bearer bar',
                'Content-type': 'application/json'
            },
            data='{"key": "value"}'
        )

    @mock.patch('backdrop.collector.write.requests')
    def test_post_to_bucket_serializes_datetimes(self, requests):
        bucket = Bucket(None, None)

        bucket.post({'key': datetime(2012, 12, 12)})

        requests.post.assert_called_with(
            url=mock.ANY,
            headers=mock.ANY,
            data='{"key": "2012-12-12T00:00:00+00:00"}'
        )
