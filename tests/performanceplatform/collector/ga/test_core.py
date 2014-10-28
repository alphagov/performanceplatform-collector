# encoding: utf-8

from datetime import date
from hamcrest import assert_that, is_, has_entries, has_item, equal_to

import mock
import datetime
import pytz

from nose.tools import (eq_)

from performanceplatform.collector.ga.core import \
    query_ga, \
    build_document_set, query_for_range, \
    query_documents_for, \
    convert_durations


# wanted something big and complicated to ensure my refactor worked
# I can delete this or tidy it up...
# maybe use something with few options as the end to end
@mock.patch("performanceplatform.collector.ga.core.query_for_range")
def test_parses_ga_data_correctly(mock_query_in_range):
    ga_data = [
        {
            'metrics': {u'visitors': u'14'},
            'start_date': datetime.date(2014, 10, 13),
            'end_date': datetime.date(2014, 10, 19),
            'dimensions': {
                u'browserVersion': u'9.4.1.482', u'browser': u'UC Browser'
            }
        },
        {
            'metrics': {u'visitors': u'14'},
            'start_date': datetime.date(2014, 10,     13),
            'end_date': datetime.date(2014, 10, 19),
            'dimensions': {
                u'browserVersion': u'9.7.6.428', u'browser': u'UC Browser'
            }
        }
    ]
    ga_data_generator = (item for item in ga_data)
    mock_query_in_range.return_value = ga_data_generator
    query = {u'metrics': [u'visitors'], u'maxResults': 0, u'dimensions': [u'browser', u'browserVersion'], u'filters': [u'customVarValue9=~^<EA570>'], u'id': u'ga:74473500'}  # noqa
    options = {u'filtersets': [[u'customVarValue9=~^<D1>'], [u'customVarValue9=~^<D2>'], [u'customVarValue9=~^<D3>'], [u'customVarValue9=~^<D4>'], [u'customVarValue9=~^<D5>'], [u'customVarValue9=~^<D6>'], [u'customVarValue9=~^<D7>'], [u'customVarValue9=~^<D8>'], [u'customVarValue9=~^<D9>'], [u'customVarValue9=~^<D10>'], [u'customVarValue9=~^<D11>'], [u'customVarValue9=~^<D12>'], [u'customVarValue9=~^<D13>'], [u'customVarValue9=~^<D14>'], [u'customVarValue9=~^<D15>'], [u'customVarValue9=~^<D16>'], [u'customVarValue9=~^<D17>'], [u'customVarValue9=~^<D18>'], [u'customVarValue9=~^<D19>'], [u'customVarValue9=~^<D23>'], [u'customVarValue9=~^<D24>'], [u'customVarValue9=~^<D25>'], [u'customVarValue9=~^<EA74>'], [u'customVarValue9=~^<EA75>'], [u'customVarValue9=~^<EA79>'], [u'customVarValue9=~^<OT532>'], [u'customVarValue9=~^<OT537>'], [u'customVarValue9=~^<D20>'], [u'customVarValue9=~^<D21>'], [u'customVarValue9=~^<D22>'], [u'customVarValue9=~^<D241>'], [u'customVarValue9=~^<D117>'], [u'customVarValue9=~^<EA199>'], [u'customVarValue9=~^<D98>'], [u'customVarValue9=~^<EA321>'], [u'customVarValue9=~^<OT554>'], [u'customVarValue9=~^<EA570>']], 'additionalFields': {'department': 'driver-and-vehicle-standards-agency'}, u'plugins': [u"Comment('department computed from filtersets by collect-content-dashboard-table.py')", u"ComputeIdFrom('_timestamp', 'timeSpan', 'dataType', 'department', 'browser', 'browserVersion')"]}  # noqa
    data_type = "browsers"
    start_date = None
    end_date = None
    results = query_documents_for(
        {},
        query,
        options,
        data_type,
        start_date,
        end_date)
    expected_results = [{u'browserVersion': u'9.4.1.482', '_timestamp': datetime.datetime(2014, 10, 13, 0, 0, tzinfo=pytz.UTC), 'timeSpan': 'week', 'dataType': 'browsers', u'visitors': 14, 'humanId': '20141013000000_week_browsers_driver-and-vehicle-standards-agency_UC Browser_9.4.1.482', 'department': 'driver-and-vehicle-standards-agency', '_id': 'MjAxNDEwMTMwMDAwMDBfd2Vla19icm93c2Vyc19kcml2ZXItYW5kLXZlaGljbGUtc3RhbmRhcmRzLWFnZW5jeV9VQyBCcm93c2VyXzkuNC4xLjQ4Mg==', u'browser': u'UC Browser'}, {u'browserVersion': u'9.7.6.428', '_timestamp': datetime.datetime(2014, 10, 13, 0, 0, tzinfo=pytz.UTC), 'timeSpan': 'week', 'dataType': 'browsers', u'visitors': 14, 'humanId': '20141013000000_week_browsers_driver-and-vehicle-standards-agency_UC Browser_9.7.6.428', 'department': 'driver-and-vehicle-standards-agency', '_id': 'MjAxNDEwMTMwMDAwMDBfd2Vla19icm93c2Vyc19kcml2ZXItYW5kLXZlaGljbGUtc3RhbmRhcmRzLWFnZW5jeV9VQyBCcm93c2VyXzkuNy42LjQyOA==', u'browser': u'UC Browser'}]  # noqa

    assert_that(results, equal_to(expected_results))


def test_query_ga_with_empty_response():
    config = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"],
        "filters": ["some-filter"]
    }
    client = mock.Mock()
    client.query.get.return_value = []

    response = query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7))

    client.query.get.assert_called_once_with(
        "123",
        date(2013, 4, 1),
        date(2013, 4, 7),
        ["visits"],
        ["date"],
        ["some-filter"],
        None,
        None,
    )

    eq_(response, [])


def test_filters_are_optional_for_querying():
    config = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"]
    }
    client = mock.Mock()

    query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7))

    client.query.get.assert_called_once_with(
        "123",
        date(2013, 4, 1),
        date(2013, 4, 7),
        ["visits"],
        ["date"],
        None,
        None,
        None,
    )


def test_dimensions_are_optional_for_querying():
    config = {
        "id": "ga:123",
        "metrics": ["visits"],
        "filters": ["some-filter"]
    }
    client = mock.Mock()

    query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7))

    client.query.get.assert_called_once_with(
        "123",
        date(2013, 4, 1),
        date(2013, 4, 7),
        ["visits"],
        None,
        ["some-filter"],
        None,
        None
    )


def test_query_for_range():
    query = {
        "id": "12345",
        "metrics": ["visits", "visitors"],
        "dimensions": ["dimension1", "dimension2"],
    }
    expected_response_0 = {
        "visits": "1234",
        "visitors": "321",
        "dimension1": "value1",
        "dimension2": "value2",
        "start_date": date(2013, 4, 1),
        "end_date": date(2013, 4, 7),
    }
    expected_response_1 = {
        "visits": "2345",
        "visitors": "5678",
        "dimension1": "value1",
        "dimension2": "value2",
        "start_date": date(2013, 4, 8),
        "end_date": date(2013, 4, 16),
    }

    client = mock.Mock()
    client.query.get.side_effect = [
        [expected_response_0],
        [expected_response_1],
    ]

    items = list(
        query_for_range(client, query, date(2013, 4, 1), date(2013, 4, 8)))

    assert_that(len(items), is_(2))

    response_0, response_1 = items

    assert_that(response_0, is_(expected_response_0))
    assert_that(response_1, is_(expected_response_1))


def test_build_document_set():
    def build_gapy_response(visits):
        return {
            "metrics": visits,
            "dimensions": {"date": "2013-04-02"},
            "start_date":  date(2013, 4, 1)
        }

    results = [
        build_gapy_response({"visits": "12345", "avgSessionDuration": 400.0}),
        build_gapy_response(
            {"visits": "2313.13", "avgSessionDuration": "400.0"}),
        build_gapy_response({"visits": "4323", "avgSessionDuration": 1234}),
    ]
    docs = list(build_document_set(results))

    assert_that(docs, has_item(has_entries({
        "visits": 12345,
        "avgSessionDuration": 400000.0
    })))
    assert_that(docs, has_item(has_entries({
        "visits": 2313.13,
        "avgSessionDuration": 400000.0
    })))
    assert_that(docs, has_item(has_entries({
        "visits": 4323,
        "avgSessionDuration": 1234000
    })))


def test_query_ga_with_sort():
    config = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"],
        "filters": ["some-filter"],
        "sort": ["-foo"],
    }
    client = mock.Mock()
    client.query.get.return_value = []

    response = list(
        query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7)))

    client.query.get.assert_called_once_with(
        "123",
        date(2013, 4, 1),
        date(2013, 4, 7),
        ["visits"],
        ["date"],
        ["some-filter"],
        None,
        ["-foo"],
    )

    eq_(response, [])


def test_query_ga_with_maxresults():
    config = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"],
        "filters": ["some-filter"],
        "maxResults": 1000,
    }
    client = mock.Mock()
    client.query.get.return_value = []

    response = query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7))

    client.query.get.assert_called_once_with(
        "123",
        date(2013, 4, 1),
        date(2013, 4, 7),
        ["visits"],
        ["date"],
        ["some-filter"],
        1000,
        None,
    )

    eq_(response, [])


def test_convert_duration():
    query_0 = ('avgSessionDuration', 400.0)
    expected_response_0 = ('avgSessionDuration', 400000.0)
    query_1 = ('avgSessionDuration', None)
    expected_response_1 = ('avgSessionDuration', None)
    query_2 = ('uniquePageviews', 400)
    expected_response_2 = ('uniquePageviews', 400)

    response_0 = convert_durations(query_0)
    response_1 = convert_durations(query_1)
    response_2 = convert_durations(query_2)

    assert_that(response_0, is_(expected_response_0))
    assert_that(response_1, is_(expected_response_1))
    assert_that(response_2, is_(expected_response_2))
