# encoding: utf-8

from datetime import date
from hamcrest import assert_that, is_, has_entries, has_item, equal_to

import mock

from nose.tools import (assert_in, assert_not_in, assert_is_instance,
                        raises, eq_)

from performanceplatform.collector.ga.core import \
    query_ga, build_document, data_id, apply_key_mapping, \
    build_document_set, query_for_range, \
    query_documents_for, map_multi_value_fields

from tests.performanceplatform.collector.ga import dt


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

    items = query_for_range(client, query, date(2013, 4, 1), date(2013, 4, 8))

    assert_that(len(items), is_(2))

    response_0, response_1 = items

    assert_that(response_0, is_(expected_response_0))
    assert_that(response_1, is_(expected_response_1))


def test_data_id():
    assert_that(
        data_id("a", dt(2012, 1, 1, 12, 0, 0, "UTC"), "week", ['one', 'two']),
        is_(
            ("YV8yMDEyMDEwMTEyMDAwMF93ZWVrX29uZV90d28=",
             "a_20120101120000_week_one_two")
        )
    )


def test_unicode_data_id():
    base64, human = data_id(
        "a",
        dt(2012, 1, 1, 12, 0, 0, "UTC"),
        "week",
        ['one', u"© ☯ ☮"])

    assert_is_instance(human, str)
    assert_that(human, is_(str("a_20120101120000_week_one_© ☯ ☮")))
    assert_that(base64,
                is_("YV8yMDEyMDEwMTEyMDAwMF93ZWVrX29uZV_CqSDimK8g4piu"))


def test_build_document():
    gapy_response = {
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02"},
        "start_date":  date(2013, 4, 1),
    }

    data = build_document(gapy_response, "weeklyvisits")

    assert_that(data, has_entries({
        "_id": "d2Vla2x5dmlzaXRzXzIwMTMwNDAxMDAwMDAwX3dlZWtfMjAxMy0wNC0wMg==",
        "humanId": 'weeklyvisits_20130401000000_week_2013-04-02',
        "dataType": "weeklyvisits",
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "timeSpan": "week",
        "date": "2013-04-02",
        "visits": 12345,
    }))


def test_build_document_no_dimensions():
    gapy_response = {
        "metrics": {"visits": "12345", "visitors": "5376"},
        "start_date": date(2013, 4, 1),
    }

    data = build_document(gapy_response, "foo")

    assert_that(data, has_entries({
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "timeSpan": "week",
        "visits": 12345,
        "visitors": 5376,
    }))


def test_build_document_mappings_are_applied_to_dimensions():
    mappings = {
        "customVarValue1": "name"
    }
    gapy_response = {
        "metrics": {"visits": "12345"},
        "dimensions": {"customVarValue1": "Jane"},
        "start_date": date(2013, 4, 1),
    }

    doc = build_document(gapy_response, "people",  mappings)

    assert_that(doc, has_entries({
        "name": "Jane"
    }))


def test_build_document_with_multi_value_field_mappings():
    mappings = {
        "multiValuesField": "originalField",
        "multiValuesField_0": "first",
        "multiValuesField_1": "second",
        "multiValuesField_2": "third",
    }

    gapy_response = {
        "metrics": {"visits": "12345"},
        "dimensions": {
            "multiValuesField": "first value:second value:third value"
        },
        "start_date": date(2013, 4, 1),
    }

    doc = build_document(gapy_response, "multival", mappings)

    assert_that(doc, has_entries({
        "originalField": "first value:second value:third value",
        "first": "first value",
        "second": "second value",
        "third": "third value",
    }))


def test_apply_key_mapping():
    mapping = {"a": "b"}

    document = apply_key_mapping(mapping, {"a": "foo", "c": "bar"})

    assert_that(document, is_({"b": "foo", "c": "bar"}))


def test_map_available_multi_value_fields():
    mapping = {
        'key_0_not_really': 'no_a_multi_key',
        'key_0': 'one',
        'key_1': 'two',
        'key_2': 'not_in_value',
        'no_key_0': 'dont_exist'
    }

    document = map_multi_value_fields(mapping, {'key': 'foo:bar'})

    assert_that(document, is_({'one': 'foo', 'two': 'bar'}))


def test_build_document_set():
    def build_gapy_response(visits, name, start_date):
        return {
            "metrics": {"visits": visits},
            "dimensions": {"customVarValue1": name},
            "start_date": start_date,
        }

    mappings = {
        "customVarValue1": "name"
    }
    results = [
        build_gapy_response("12345", "Jane", date(2013, 4, 1)),
        build_gapy_response("2313", "John", date(2013, 4, 1)),
        build_gapy_response("4323", "Joanne", date(2013, 4, 8)),
    ]
    docs = build_document_set(results, "people", mappings)

    assert_that(docs, has_item(has_entries({
        "name": "Jane",
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "dataType": "people",
        "visits": 12345,
    })))
    assert_that(docs, has_item(has_entries({
        "name": "John",
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "visits": 2313,
    })))
    assert_that(docs, has_item(has_entries({
        "name": "Joanne",
        "_timestamp": dt(2013, 4, 8, 0, 0, 0, "UTC"),
        "visits": 4323,
    })))


@raises(ValueError)
def test_build_document_fails_with_no_data_type():
    build_document({}, None)


def test_if_we_provide_id_field_it_is_used():
    doc = build_document({"dimensions": {"idVar": "foo"},
                          "metrics": {"some_metric": 123},
                          "start_date": date(2014, 2, 19)},
                         "data_type",
                         idMapping="idVar")

    eq_(doc["_id"], "Zm9v")


def test_if_we_provide_id_field_array_it_is_used():
    doc = build_document({"dimensions": {"a": u"1؁", "b": u"2"},
                          "metrics": {"some_metric": "123"},
                          "start_date": date(2014, 2, 19)},
                         "data_type",
                         idMapping=["a", "b"])

    eq_(doc["_id"], "MdiBMg==")


def test_plugin():

    input_document = {
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02", "customVarValue9": "foo"},
        "start_date": date(2013, 4, 1),
    }

    client = mock.Mock()
    client.query.get.return_value = [
        input_document,
    ]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date", "customVarValue9"],
    }
    data_type = "test"
    options = {}

    start, end = date(2013, 4, 1), date(2013, 4, 7)

    # Check that without a plugin, we have customVarValue9.
    result = query_documents_for(client, query, options, data_type, start, end)
    (output_document,) = result
    assert_in("customVarValue9", result[0])

    # Add the plugin
    options["plugins"] = [
        'RemoveKey("customVarValue9")',
        'ComputeIdFrom("date")',
    ]

    # Check that plugin has desired effect
    result = query_documents_for(client, query, options, data_type, start, end)
    (output_document,) = result
    assert_not_in("customVarValue9", result[0])


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

    response = query_ga(client, config, date(2013, 4, 1), date(2013, 4, 7))

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


def test_additional_fields():

    input_document = {
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02", "customVarValue9": "foo"},
        "start_date": date(2013, 4, 1),
    }

    client = mock.Mock()
    client.query.get.return_value = [
        input_document,
    ]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date", "customVarValue9"],
    }
    data_type = "test"
    options = {
        "additionalFields": {"foo": "bar"},
    }

    start, end = date(2013, 4, 1), date(2013, 4, 7)

    # Check that foo is set on the output document
    result = query_documents_for(client, query, options, data_type, start, end)
    (output_document,) = result
    assert_in("foo", result[0])
    assert_that(result[0]["foo"], equal_to("bar"))


def test_float_number():
    query = {
        "id": "12345",
        "metrics": ["rate"],
        "dimensions": [],
    }
    expected_response_0 = {
        "start_date": date(2013, 4, 1),
        "end_date": date(2013, 4, 7),
        "metrics": {"rate": "23.4"},
    }

    client = mock.Mock()
    client.query.get.side_effect = [
        [expected_response_0],
    ]

    items = query_for_range(client, query, date(2013, 4, 1), date(2013, 4, 7))

    assert_that(len(items), is_(1))
    (response_0,) = items
    assert_that(response_0, is_(expected_response_0))

    (doc_0,) = build_document_set(items, "", None, None)

    assert_that(doc_0['rate'], is_(23.4))
