import unittest
from performanceplatform.collector.webtrends.keymetrics import(
    Collector, Parser)
import performanceplatform
import json
from mock import patch
import requests
from hamcrest import assert_that, equal_to, has_entries
from nose.tools import assert_raises
import datetime
import pytz
from tests.performanceplatform.collector.ga import dt


def build_collector(start_at=None,
                    end_at=None,
                    query=None):
    if not query:
        query = {}
    credentials = {
        'user': 'abc',
        'password': 'def',
        'keymetrics_url': 'http://this.com/'
    }
    return Collector(credentials, query, start_at, end_at)


def get_fake_response():
    with open("tests/fixtures/"
              "webtrends_keymetrics_last_3_hours.json", "r") as f:
        return json.loads(f.read())


class TestCollector(unittest.TestCase):
    @patch.object(
        performanceplatform.client.DataSet, "post")
    @patch(
        "performanceplatform.collector.webtrends"
        ".keymetrics.requests_with_backoff.get")
    def test_collect_parse_and_push(self, mock_get, mock_post):
        mock_get().json.return_value = get_fake_response()
        query = {}
        options = {
            'mappings': {'Visitors': 'visitors',
                         'Page Views per Visit': 'page_views_per_visit'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "test"]}
        collector = build_collector(query=query)
        collector.collect_parse_and_push(
            {'data-type': 'realtime',
             'url': 'abc',
             'token': 'def',
             'dry_run': False},
            options)
        posted_data = [
            {
                "_id": "cmVhbHRpbWUyMDE0LTEwLTMwIDEzOjAwOjAwKzAwOjAwZmllbGQ=",
                "_timestamp": datetime.datetime(
                    2014, 10, 30, 13, 0, tzinfo=pytz.UTC),
                "dataType": "realtime",
                "humanId": "realtime2014-10-30 13:00:00+00:00field",
                "visitors": 2.0,
                "page_views_per_visit": 4.0,
                u'Avg. Time on Site': 0.0,
                u'Page Views': 12.0,
                u'New Visitors': 1.0,
                u'Visits': 3.0,
                u'Avg. Visitors per Day': 0.0,
                u'Bounce Rate': 0.0,
                "test": "field"
            },
            {
                "_id": "cmVhbHRpbWUyMDE0LTEwLTMwIDEyOjAwOjAwKzAwOjAwZmllbGQ=",
                "_timestamp": datetime.datetime(
                    2014, 10, 30, 12, 0, tzinfo=pytz.UTC),
                "dataType": "realtime",
                "humanId": "realtime2014-10-30 12:00:00+00:00field",
                "visitors": 1.0,
                "page_views_per_visit": 9.50,
                u'Avg. Time on Site': 0.0,
                u'Page Views': 19.0,
                u'New Visitors': 0.0,
                u'Visits': 2.0,
                u'Avg. Visitors per Day': 0.0,
                u'Bounce Rate': 0.0,
                "test": "field"
            }
        ]
        mock_post.assert_called_once_with(posted_data, chunk_size=100)

    @patch("performanceplatform.collector.webtrends"
           ".keymetrics.requests_with_backoff.get")
    def test_collect_when_specified_start_and_end_and_hourly(self, mock_get):
        mock_get().json.return_value = get_fake_response()
        # as the above bit of setup is a call
        mock_get.reset_mock()
        collector = build_collector("2014-08-03", "2014-08-05")
        assert_that(collector.collect(), equal_to([
            get_fake_response()["data"]]))
        mock_get.assert_called_once_with(
            url="http://this.com/",
            auth=('abc', 'def'),
            params={
                'start_period': "2014m08d03",
                'end_period': "2014m08d05",
                'format': 'json',
                'userealtime': True
            })

    @patch("performanceplatform.collector.webtrends"
           ".keymetrics.requests_with_backoff.get")
    def test_collect_when_no_specified_start_and_end(self, mock_get):
        mock_get().json.return_value = get_fake_response()
        # as the above bit of setup is a call
        mock_get.reset_mock()
        collector = build_collector()
        assert_that(
            collector.collect(),
            equal_to([get_fake_response()["data"]]))
        mock_get.assert_called_once_with(
            url="http://this.com/",
            auth=('abc', 'def'),
            params={
                'start_period': "current_hour-1",
                'end_period': "current_hour-1",
                'format': 'json',
                'userealtime': True
            }
        )

    @patch("performanceplatform.collector.webtrends"
           ".keymetrics.requests_with_backoff.get")
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
                ("2014m08d03", "2014m08d05"),
            ])
        )

    def test_collect_builds_date_ranges_correctly_when_no_start_and_end(self):
        assert_that(
            Collector.date_range_for_webtrends(),
            equal_to([
                ("current_hour-1", "current_hour-1")
            ])
        )


class TestParser(unittest.TestCase):
    def test_handles_returned_date_format_correctly(self):
        assert_that(
            Parser.to_datetime(
                "10/30/2014 13"),
            equal_to(dt(2014, 10, 30, 13, 0, 0, "UTC")))

    def test_handles_no_data_in_period(self):
        options = {
            'row_type_name': 'browser',
            'mappings': {'Visits': 'visitors'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "browser"]}
        data_type = "realtime"
        parser = Parser(options, data_type)
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
                "_id": "cmVhbHRpbWUyMDE0LTEwLTMwIDEyOjAwOjAwKzAwOjAwZmllbGQ=",
                "_timestamp": datetime.datetime(
                    2014, 10, 30, 12, 0, tzinfo=pytz.UTC),
                "dataType": "realtime",
                "humanId": "realtime2014-10-30 12:00:00+00:00field",
                "visitors": 1.0,
                "page_views_per_visit": 9.50,
                "test": "field"
            },
            {
                "_id": "cmVhbHRpbWUyMDE0LTEwLTMwIDEzOjAwOjAwKzAwOjAwZmllbGQ=",
                "_timestamp": datetime.datetime(
                    2014, 10, 30, 13, 0, tzinfo=pytz.UTC),
                "dataType": "realtime",
                "humanId": "realtime2014-10-30 13:00:00+00:00field",
                "visitors": 2.0,
                "page_views_per_visit": 4.0,
                "test": "field"
            }
        ]
        options = {
            'mappings': {'Visitors': 'visitors',
                         'Page Views per Visit': 'page_views_per_visit'},
            'additionalFields': {'test': 'field'},
            # does it matter that this is joined by blank string?
            'idMapping': ["dataType", "_timestamp", "test"]}
        data_type = "realtime"
        parser = Parser(options, data_type)
        results = list(parser.parse([get_fake_response()['data']]))
        assert_that(results[0], has_entries(posted_data[1]))
        assert_that(results[1], has_entries(posted_data[0]))
