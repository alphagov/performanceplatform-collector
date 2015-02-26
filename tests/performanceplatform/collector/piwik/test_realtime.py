import unittest
import json
from mock import patch
from hamcrest import assert_that, equal_to
from freezegun import freeze_time
from performanceplatform.collector.piwik.realtime import main, Fetcher, Parser
from tests.performanceplatform.collector.piwik.test_core import(
    query, credentials)


def get_piwik_data():
    with open("tests/fixtures/piwik_realtime.json", "r") as f:
        return json.loads(f.read())


def build_fetcher(query_params={}):
    q = query()
    q.update(query_params)
    return Fetcher(credentials(), q)


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
                'method': 'SomeNamespace.someMethod',
                'lastMinutes': 2
            }
        )

    def test_minutes_option_sets_last_minutes_request_param(self, mock_get):
        fetcher = build_fetcher({'minutes': 30})
        fetcher.fetch()
        minutes = mock_get.call_args[1]['params']['lastMinutes']
        assert_that(minutes, equal_to(30))


class TestParser(unittest.TestCase):
    def test_parses_piwik_visitors_metric(self):
        parser = Parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['unique_visitors'], equal_to(53))

    @freeze_time("2015-02-16 12:00:01")
    def test_sets_timestamp_to_now(self):
        parser = Parser()
        parsed_data = parser.parse(get_piwik_data())
        assert_that(parsed_data[0]['_timestamp'],
                    equal_to('2015-02-16T12:00:01+00:00'))


class TestMain(unittest.TestCase):
    def test_main_disallows_start_at_and_end_at_date_arguments(self):
        with self.assertRaises(SystemExit) as cm:
            main(credentials(),
                 {'data-type': 'realtime'},
                 query(),
                 {},
                 '2015-01-21',
                 '2015-01-22')
        self.assertEqual(cm.exception.code, 1)

    @patch("performanceplatform.collector.piwik.realtime.Pusher")
    @patch("performanceplatform.collector.piwik.base."
           "requests_with_backoff.get")
    def test_main_fetches_and_pushes_parsed_data(self, mock_get, mock_pusher):
        mock_get().json.return_value = get_piwik_data()
        parser = Parser()
        parsed_data = parser.parse(get_piwik_data())
        main(credentials(),
             {'data-type': 'realtime'}, query(), {}, None, None)
        second_call = mock_pusher.mock_calls[1]
        argument = second_call[1][0]
        assert_that(argument, equal_to(parsed_data))
