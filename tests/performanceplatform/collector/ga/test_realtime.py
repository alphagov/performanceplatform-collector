from freezegun import freeze_time
from mock import Mock, patch, ANY
from nose.tools import *
import performanceplatform
from performanceplatform.collector.ga.realtime import Collector, Realtime, _timestamp
from hamcrest.library.text.stringmatches import matches_regexp
from hamcrest.library.integration import match_equality
import re
import json

TIMESTAMP_PATTERN = re.compile(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\+\d\d')


def mock_instance(cls):
    mock = Mock()
    cls.return_value = mock
    return mock


def is_timestamp():
    return match_equality(matches_regexp(TIMESTAMP_PATTERN))


def fetch_realtime_response():
    with open("tests/fixtures/realtime_response.json", "r") as f:
        return json.loads(f.read())


class TestCollector(object):
    @patch("performanceplatform.collector.ga.realtime.DataSet")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "_authenticate")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "execute_ga_query")
    def test_send_records_for_winter_real_response(self, execute_ga_query, authenticate, DataSet):
        execute_ga_query.return_value = fetch_realtime_response()
        data_set = mock_instance(DataSet)
        DataSet.from_config.return_value = data_set

        collector = Collector({"CLIENT_SECRETS": None, "STORAGE_PATH": None})
        data_set_config = {"url": 'url', "token": 'token'}

        # this utc time corresponds to GMT - winter
        with freeze_time("2014-01-07 10:20:57", tz_offset=0):
            collector.send_records_for({},
                                       to=data_set_config)

        DataSet.from_config.assert_called_with(data_set_config)

        data_set.post.assert_called_with({
            '_timestamp': '2014-01-07T10:20:57+00:00',
            'for_url': '',
            'unique_visitors': 20459,
            '_id': '2014-01-07T10:20:57+00:00'
        })

    @patch("performanceplatform.collector.ga.realtime.DataSet")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "_authenticate")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "execute_ga_query")
    def test_send_records_for_summer_real_response(self, execute_ga_query, authenticate, DataSet):
        execute_ga_query.return_value = fetch_realtime_response()
        data_set = mock_instance(DataSet)
        DataSet.from_config.return_value = data_set

        collector = Collector({"CLIENT_SECRETS": None, "STORAGE_PATH": None})
        data_set_config = {"url": 'url', "token": 'token'}

        # this utc time corresponds to BST - summer
        with freeze_time("2014-04-07 10:20:57", tz_offset=0):
            collector.send_records_for({},
                                       to=data_set_config)

        DataSet.from_config.assert_called_with(data_set_config)

        data_set.post.assert_called_with({
            '_timestamp': '2014-04-07T10:20:57+00:00',
            'for_url': '',
            'unique_visitors': 20459,
            '_id': '2014-04-07T10:20:57+00:00'
        })

    @patch("performanceplatform.collector.ga.realtime.DataSet")
    @patch("performanceplatform.collector.ga.realtime.Realtime")
    def test_send_records_for(self, Realtime, DataSet):
        realtime = mock_instance(Realtime)
        data_set = mock_instance(DataSet)
        DataSet.from_config.return_value = data_set

        realtime.query.return_value = 12

        collector = Collector('credentials')
        data_set_config = {"url": 'url', "token": 'token'}

        collector.send_records_for({},
                                   to=data_set_config)

        Realtime.assert_called_with('credentials')
        DataSet.from_config.assert_called_with(data_set_config)

        realtime.query.assert_called_with({})
        data_set.post.assert_called_with({
            '_timestamp': is_timestamp(),
            '_id': is_timestamp(),
            'unique_visitors': 12,
            'for_url': '',
        })

    @patch("performanceplatform.collector.ga.realtime.Realtime")
    def test_sending_records_fails_with_invalid_target(self, _):
        collector = Collector(None)
        assert_raises(KeyError, collector.send_records_for,
                      None, {'foo': 'bar'})

    @patch("performanceplatform.collector.ga.realtime.DataSet")
    @patch("performanceplatform.collector.ga.realtime.Realtime")
    def test_sending_records_sends_for_url(self, _, DataSet):
        data_set = mock_instance(DataSet)
        DataSet.from_config.return_value = data_set

        collector = Collector(None)
        data_set_config = {"url": 'url', "token": 'token'}
        collector.send_records_for({'filters': 'myurl'},
                                   to=data_set_config)

        data_set.post.assert_called_with({
            '_timestamp': ANY,
            '_id': ANY,
            'unique_visitors': ANY,
            'for_url': 'myurl',
        })


def test_timestamp_in_summer_remains_utc():
    with freeze_time("2014-04-07 10:20:57", tz_offset=0):
        assert_equal(_timestamp(), "2014-04-07T10:20:57+00:00")


def test_timestamp_in_winter_remains_utc():
    with freeze_time("2014-01-07 10:20:57", tz_offset=0):
        assert_equal(_timestamp(), "2014-01-07T10:20:57+00:00")


class TestRealtime(object):
    """No tests for Realtime authentication
    This class just deals with the google analytics client. Testing it would
    require a lot of mocking and would be quite brittle.
    """
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "_authenticate")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "execute_ga_query")
    def test_valid_ga_response_parses_active_visitors_correctly(
            self, execute_ga_query, authenticate):
        execute_ga_query.return_value = fetch_realtime_response()
        realtime = Realtime({"CLIENT_SECRETS": None, "STORAGE_PATH": None})
        value = realtime.query(None)
        assert_equal(value, 20459)

    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "_authenticate")
    @patch.object(performanceplatform.collector.ga.realtime.Realtime, "execute_ga_query")
    def test_should_return_zero_if_no_rows_returned_from_ga(
            self, execute_ga_query, authenticate):
        execute_ga_query.return_value = {}
        realtime = Realtime({"CLIENT_SECRETS": None, "STORAGE_PATH": None})
        value = realtime.query(None)
        assert_equal(value, 0)
