import unittest
from performanceplatform.collector.webtrends.reports import Collector, Parser
import performanceplatform
import json
from mock import patch, call
import requests
from hamcrest import assert_that, equal_to, has_entries
from nose.tools import assert_raises
import datetime
import pytz


def build_collector(start_at=None,
                    end_at=None,
                    query=None):
    if not query:
        query = {'report_id': 'whoop'}
    credentials = {
        'user': 'abc',
        'password': 'def',
        'reports_url': 'http://this.com/'
    }
    return Collector(credentials, query, start_at, end_at)


def get_fake_response():
    with open("tests/fixtures/webtrends_day_one.json", "r") as f:
        return json.loads(f.read())


class TestCollector(unittest.TestCase):
    @patch.object(
        performanceplatform.client.DataSet, "post")
    @patch(
        "performanceplatform.collector.webtrends"
        ".reports.requests_with_backoff.get")
    def test_collect_parse_and_push(self, mock_get, mock_post):
        mock_get().json.return_value = get_fake_response()
        query = {'frequency': 'daily', 'report_id': 'whoop'}
        options = {
            'row_type_name': 'browser',
            'mappings': {'Visits': 'visitors'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "timeSpan", "browser"]}
        collector = build_collector(query=query)
        collector.collect_parse_and_push(
            {'data-type': 'browsers',
             'url': 'abc',
             'token': 'def',
             'dry_run': False},
            options)
        posted_data = [
            {
                "_id": "YnJvd3NlcnMyMDE0LTEwLTE0IDAw"
                       "OjAwOjAwKzAwOjAwZGF5TW96aWxsYQ==",
                "_timestamp": datetime.datetime(
                    2014, 10, 14, 0, 0, tzinfo=pytz.UTC),
                "browser": "Mozilla",
                "dataType": "browsers",
                "humanId": "browsers2014-10-14 00:00:00+00:00dayMozilla",
                # day legit?
                "timeSpan": "day",
                "visitors": 1,
                "test": "field"
            },
            {
                "_id": "YnJvd3NlcnMyMDE0LTEwLTE0IDAw"
                       "OjAwOjAwKzAwOjAwZGF5R29vZ2xlIENocm9tZQ==",
                "_timestamp": datetime.datetime(
                    2014, 10, 14, 0, 0, tzinfo=pytz.UTC),
                "browser": "Google Chrome",
                "dataType": "browsers",
                "humanId": "browsers2014-10-14 00:00:00+00:00dayGoogle Chrome",
                "timeSpan": "day",
                "visitors": 18,
                "test": "field"
            }
        ]
        mock_post.assert_called_once_with(posted_data, chunk_size=100)

    @patch(
        "performanceplatform.collector.webtrends"
        ".reports.requests_with_backoff.get")
    def test_collect_when_specified_start_and_end_and_hourly(self, mock_get):
        pass

    @patch("performanceplatform.collector.webtrends"
           ".reports.requests_with_backoff.get")
    def test_collect_when_specified_start_and_end_and_daily(self, mock_get):
        mock_get().json.return_value = get_fake_response()
        # as the above bit of setup is a call
        mock_get.reset_mock()
        collector = build_collector("2014-08-03", "2014-08-05")
        assert_that(collector.collect(), equal_to([
            get_fake_response()["data"],
            get_fake_response()["data"],
            get_fake_response()["data"]]))
        calls = [
            call(
                url="http://this.com/whoop",
                auth=('abc', 'def'),
                params={
                    'start_period': "2014m08d03",
                    'end_period': "2014m08d04",
                    'format': 'json'
                }
            ),
            call(
                url="http://this.com/whoop",
                auth=('abc', 'def'),
                params={
                    'start_period': "2014m08d04",
                    'end_period': "2014m08d05",
                    'format': 'json'}
            ),
            call(
                url="http://this.com/whoop",
                auth=('abc', 'def'),
                params={
                    'start_period': "2014m08d05",
                    'end_period': "2014m08d06",
                    'format': 'json'
                })
        ]
        assert_that(mock_get.call_args_list[0], equal_to(calls[0]))
        assert_that(mock_get.call_args_list[1], equal_to(calls[1]))
        assert_that(mock_get.call_args_list[2], equal_to(calls[2]))

    @patch("performanceplatform.collector.webtrends"
           ".reports.requests_with_backoff.get")
    def test_collect_when_no_specified_start_and_end(self, mock_get):
        mock_get().json.return_value = get_fake_response()
        # as the above bit of setup is a call
        mock_get.reset_mock()
        collector = build_collector()
        assert_that(
            collector.collect(),
            equal_to([get_fake_response()["data"]]))
        mock_get.assert_called_once_with(
            url="http://this.com/whoop",
            auth=('abc', 'def'),
            params={
                'start_period': "current_day-2",
                'end_period': "current_day-1",
                'format': 'json'
            }
        )

    @patch("performanceplatform.collector.webtrends"
           ".reports.requests_with_backoff.get")
    def test_collect_correctly_raises_for_status(self, mock_get):
        response = requests.Response()
        response.status_code = 400
        mock_get.return_value = response
        collector = build_collector()
        assert_raises(requests.exceptions.HTTPError, collector.collect)

    def test_collect_parses_start_and_end_date_format_correctly(self):
        date = Collector.parse_standard_date_string_to_date("2014-08-03")
        assert_that(
            Collector.parse_date_for_query(date), equal_to("2014m08d03"))
        date = Collector.parse_standard_date_string_to_date("2014-12-19")
        assert_that(
            Collector.parse_date_for_query(date), equal_to("2014m12d19"))

    def test_parse_standard_date_string_to_date_leaves_datetimes(self):
        date = Collector.parse_standard_date_string_to_date("2014-12-19")
        date_unchanged = Collector.parse_standard_date_string_to_date(date)
        assert_that(date_unchanged, equal_to(date))
        assert_that(type(date), datetime.datetime)

    def test_collect_builds_date_ranges_correctly_when_start_and_end_given(
            self):
        assert_that(Collector.date_range_for_webtrends(
            "2014-08-03",
            "2014-08-05"),
            equal_to([
                ("2014m08d03", "2014m08d04"),
                ("2014m08d04", "2014m08d05"),
                ("2014m08d05", "2014m08d06")
            ])
        )

    def test_collect_builds_date_ranges_correctly_when_no_start_and_end(self):
        assert_that(
            Collector.date_range_for_webtrends(),
            equal_to([
                ("current_day-2", "current_day-1")
            ])
        )


class TestParser(unittest.TestCase):
    def test_handles_returned_date_format_correctly(self):
        from tests.performanceplatform.collector.ga import dt
        assert_that(
            Parser.to_datetime(
                "10/14/2014-10/15/2014"),
            equal_to(dt(2014, 10, 14, 0, 0, 0, "UTC")))

    def test_handles_no_data_in_period(self):
        query = {'frequency': 'daily', 'report_id': 'whoop'}
        options = {
            'row_type_name': 'browser',
            'mappings': {'Visits': 'visitors'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "timeSpan", "browser"]}
        data_type = "browsers"
        parser = Parser(options, query, data_type)
        no_data_response = {
            "10/14/2014-10/15/2014": {
                "SubRows": None,
                "measures": {
                    "Visits": 0.0
                }
            }
        }
        results = list(parser.parse([no_data_response]))
        assert_that(results, equal_to([]))

    def test_parses_data_correctly(self):
        posted_data = [
            {
                "_id": "YnJvd3NlcnMyMDE0LTEwLTE0IDAw"
                       "OjAwOjAwKzAwOjAwZGF5TW96aWxsYQ==",
                "_timestamp": datetime.datetime(
                    2014, 10, 14, 0, 0, tzinfo=pytz.UTC),
                "browser": "Mozilla",
                "dataType": "browsers",
                "humanId": "browsers2014-10-14 00:00:00+00:00dayMozilla",
                # day legit?
                "timeSpan": "day",
                "visitors": 1,
                "test": "field"
            },
            {
                "_id": "YnJvd3NlcnMyMDE0LTEwLTE0IDAw"
                       "OjAwOjAwKzAwOjAwZGF5R29vZ2xlIENocm9tZQ==",
                "_timestamp": datetime.datetime(
                    2014, 10, 14, 0, 0, tzinfo=pytz.UTC),
                "browser": "Google Chrome",
                "dataType": "browsers",
                "humanId": "browsers2014-10-14 00:00:00+00:00dayGoogle Chrome",
                "timeSpan": "day",
                "visitors": 18,
                "test": "field"
            }
        ]
        query = {'frequency': 'daily', 'report_id': 'whoop'}
        options = {
            'row_type_name': 'browser',
            'mappings': {'Visits': 'visitors'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "timeSpan", "browser"]}
        data_type = "browsers"
        parser = Parser(options, query, data_type)
        results = list(parser.parse([get_fake_response()['data']]))
        assert_that(results[0], has_entries(posted_data[0]))
        assert_that(results[1], has_entries(posted_data[1]))
