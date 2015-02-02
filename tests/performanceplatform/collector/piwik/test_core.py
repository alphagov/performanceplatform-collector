import unittest
import json
from datetime import datetime
import pytz
from mock import patch
import requests
from hamcrest import assert_that, equal_to, has_key
from nose.tools import assert_raises
from performanceplatform.collector.piwik.core import main, Fetcher, Parser


def get_piwik_data(period='daily'):
    file = "tests/fixtures/piwik_browser_count_{0}.json".format(period)
    with open(file, "r") as f:
        return json.loads(f.read())


def build_fetcher(query_params={}, start_at=None, end_at=None):
    q = query()
    q.update(query_params)
    return Fetcher(credentials(), q, start_at, end_at)


def query():
    return {
        'site_id': '7',
        'api_method': 'SomeNamespace.someMethod'
    }


def credentials():
    return {
        'token_auth': 'foo',
        'url': 'http://bar.com'
    }


@patch("performanceplatform.collector.piwik.base.requests_with_backoff.get")
class TestFetcher(unittest.TestCase):
    def test_all_core_request_params_are_present_and_correct(self, mock_get):
        fetcher = build_fetcher()
        fetcher.fetch()
        mock_get.assert_called_once_with(
            url='http://bar.com?module=API',
            params={
                'idSite': '7',
                'token_auth': 'foo',
                'format': 'json',
                'date': 'previous1',
                'method': 'SomeNamespace.someMethod',
                'period': 'week'
            }
        )

    def test_frequency_option_sets_period_request_param(self, mock_get):
        fetcher = build_fetcher({'frequency': 'daily'})
        fetcher.fetch()
        period = mock_get.call_args[1]['params']['period']
        assert_that(period, equal_to('day'))

    def test_date_range_job_arguments_set_date_request_param(self, mock_get):
        start_at = datetime(2015, 1, 21, 0, 0)
        end_at = datetime(2015, 1, 22, 0, 0)
        fetcher = build_fetcher(start_at=start_at, end_at=end_at)
        fetcher.fetch()
        period = mock_get.call_args[1]['params']['date']
        assert_that(period, equal_to('2015-01-21,2015-01-22'))

    def test_data_can_be_fetched(self, mock_get):
        mock_get().json.return_value = get_piwik_data()
        fetcher = build_fetcher()
        assert_that(fetcher.fetch(), equal_to(get_piwik_data()))

    def test_unsuccessful_fetch_is_handled(self, mock_get):
        response = requests.Response()
        response.status_code = 400
        mock_get.return_value = response
        fetcher = build_fetcher()
        assert_raises(requests.exceptions.HTTPError, fetcher.fetch)


def options():
    return {
        'mappings': {
            'nb_visits': 'sessions', 'label': 'browser'
        },
        'idMapping': [
            'dataType', '_timestamp', 'timeSpan', 'browser']
    }


def build_parser(query_params={}):
    q = query()
    q.update(query_params)
    data_type = 'browser-count'

    return Parser(q, options(), data_type)


class TestParser(unittest.TestCase):
    def test_parses_piwik_day_key_into_a_timestamp(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['_timestamp'],
                    equal_to(datetime(2015, 2, 13, 0, 0, tzinfo=pytz.UTC)))

    def test_parses_from_date_of_a_piwik_date_range_key_into_a_timestamp(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data('weekly'))
        assert_that(parsed_data[0]['_timestamp'],
                    equal_to(datetime(2015, 2, 2, 0, 0, tzinfo=pytz.UTC)))

    def test_parses_piwik_month_key_into_a_timestamp(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data('monthly'))
        assert_that(parsed_data[0]['_timestamp'],
                    equal_to(datetime(2015, 2, 1, 0, 0, tzinfo=pytz.UTC)))

    def test_parses_piwik_datapoints_keyed_by_multiple_dates(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data('weekly'))
        assert_that(len(parsed_data), equal_to(4))

    def test_mapping_of_piwik_datapoint_keys(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0], has_key('browser'))
        assert_that(parsed_data[0], has_key('sessions'))
        self.assertFalse('label' in parsed_data[0])
        self.assertFalse('nb_visits' in parsed_data[0])

    def test_parses_piwik_datapoint_values(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['browser'], equal_to('Chrome'))
        assert_that(parsed_data[0]['sessions'], equal_to(5634))

    def test_ignores_other_non_mapped_piwik_datapoint_keys(self):
        parser = build_parser()
        data = get_piwik_data()
        parsed_data = parser.parse(data)
        for k in data.get("2015-02-13")[0].keys():
            self.assertFalse(k in parsed_data[0])

    def test_defaults_time_span_to_weekly(self):
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['timeSpan'], equal_to('week'))

    def test_frequency_option_sets_time_span(self):
        parser = build_parser({'frequency': 'daily'})
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['timeSpan'], equal_to('day'))


class TestMain(unittest.TestCase):
    @patch("performanceplatform.collector.piwik.core.Pusher")
    @patch("performanceplatform.collector.piwik.base."
           "requests_with_backoff.get")
    def test_main_fetches_and_pushes_parsed_data(self, mock_get, mock_pusher):
        mock_get().json.return_value = get_piwik_data()
        parser = build_parser()
        parsed_data = parser.parse(get_piwik_data())
        main(credentials(),
             {'data-type': 'browser-count'}, query(), options(), None, None)
        second_call = mock_pusher.mock_calls[1]
        argument = second_call[1][0]
        assert_that(argument, equal_to(parsed_data))
