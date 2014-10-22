# encoding: utf-8
from datetime import date
from hamcrest import assert_that, is_, has_entries, has_item, equal_to

import mock

from nose.tools import (
    eq_,
    assert_raises,
    assert_is_instance,
    assert_in,
    assert_not_in)

from performanceplatform.utils.data_parser import \
    build_document, build_document_set, data_id, \
    apply_key_mapping, map_multi_value_fields, \
    DataParser

from performanceplatform.collector.ga.core import \
    build_document_set as ga_build_document_set

from performanceplatform.collector.ga.core import query_for_range
from tests.performanceplatform.collector.ga import dt


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

    items = list(
        query_for_range(client, query, date(2013, 4, 1), date(2013, 4, 7)))

    assert_that(len(items), is_(1))
    (response_0,) = items
    assert_that(response_0, is_(expected_response_0))

    special_fields = list(ga_build_document_set(items))
    (doc_0,) = build_document_set(
        items, "", None, special_fields, idMapping=None)

    assert_that(doc_0['rate'], is_(23.4))


def test_build_document_set_raises_error_if_special_fields_different_length():
    assert_raises(ValueError,
                  build_document_set,
                  [{}, {}],
                  "",
                  None,
                  [{}],
                  idMapping=None)


def test_if_we_provide_id_field_it_uses_the_whole_doc():
    doc = build_document({"dimensions": {"var": "f"},
                          "metrics": {"some_metric": 123},
                          "start_date": date(2014, 2, 19)},
                         "oo",
                         idMapping=["var", "dataType"])

    eq_(doc["_id"], "Zm9v")


def test_if_we_provide_id_field_it_includes_additional_fields():
    doc = build_document({"dimensions": {},
                          "metrics": {"some_metric": 123},
                          "start_date": date(2014, 2, 19)},
                         "data_type",
                         idMapping="idVar",
                         additionalFields={"idVar": "foo"})

    eq_(doc["_id"], "Zm9v")


def test_if_we_provide_id_field_array_it_is_used():
    doc = build_document({"dimensions": {"a": u"1؁", "b": u"2"},
                          "metrics": {"some_metric": "123"},
                          "start_date": date(2014, 2, 19)},
                         "data_type",
                         idMapping=["a", "b"])

    eq_(doc["_id"], "MdiBMg==")


def test_if_we_provide_id_field_it_is_used():
    doc = build_document({"dimensions": {"idVar": "foo"},
                          "metrics": {"some_metric": 123},
                          "start_date": date(2014, 2, 19)},
                         "data_type",
                         idMapping="idVar")

    eq_(doc["_id"], "Zm9v")


def test_build_document_fails_with_no_data_type():
    assert_raises(ValueError, build_document, {}, None)


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
    special_fields = list(ga_build_document_set(results))
    docs = build_document_set(
        results,
        "people",
        mappings,
        special_fields)

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


def test_mapping_is_applied_to_whole_document():
    mappings = {
        'visits': 'count',
        'customVarValue1': 'name',
        'timeSpan': 'period',
    }
    gapy_response = {
        "metrics": {"visits": "12345"},
        "dimensions": {"customVarValue1": "Jane"},
        "start_date": date(2013, 4, 1),
    }

    (special_fields_for_doc,) = ga_build_document_set([gapy_response])
    doc = build_document(
        gapy_response, "people", special_fields_for_doc, mappings=mappings)

    assert_that(doc, has_entries({
        "count": 12345,
        "name": "Jane",
        "period": "week",
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

    (special_fields_for_doc,) = ga_build_document_set([gapy_response])
    doc = build_document(
        gapy_response, "multival", special_fields_for_doc, mappings=mappings)

    assert_that(doc, has_entries({
        "originalField": "first value:second value:third value",
        "first": "first value",
        "second": "second value",
        "third": "third value",
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

    (special_fields_for_doc,) = ga_build_document_set([gapy_response])
    doc = build_document(
        gapy_response,
        "people",
        special_fields=special_fields_for_doc,
        mappings=mappings)

    assert_that(doc, has_entries({
        "name": "Jane"
    }))


def test_build_document_no_dimensions():
    gapy_response = {
        "metrics": {"visits": "12345", "visitors": "5376"},
        "start_date": date(2013, 4, 1),
    }

    (special_fields_for_doc,) = ga_build_document_set([gapy_response])
    data = build_document(gapy_response, "foo", special_fields_for_doc)

    assert_that(data, has_entries({
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "timeSpan": "week",
        "visits": 12345,
        "visitors": 5376,
    }))


def test_build_document():
    gapy_response = {
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02"},
        "start_date":  date(2013, 4, 1),
    }

    (special_fields_for_doc,) = ga_build_document_set([gapy_response])
    data = build_document(
        gapy_response, "weeklyvisits", special_fields_for_doc)

    assert_that(data, has_entries({
        "_id": "d2Vla2x5dmlzaXRzXzIwMTMwNDAxMDAwMDAwX3dlZWtfMjAxMy0wNC0wMg==",
        "humanId": 'weeklyvisits_20130401000000_week_2013-04-02',
        "dataType": "weeklyvisits",
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "timeSpan": "week",
        "date": "2013-04-02",
        "visits": 12345,
    }))


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


def test_plugin():
    data = [
        {
            'metrics': {'visits': '12345'},
            'dimensions': {'date': '2013-04-02', 'customVarValue9': 'foo'},
            'start_date': date(2013, 4, 1)
        }
    ]
    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date", "customVarValue9"],
    }
    data_type = "test"
    options = {}

    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))

    assert_in("customVarValue9", result[0])

    # Add the plugin
    options["plugins"] = [
        'RemoveKey("customVarValue9")',
        'ComputeIdFrom("date")',
    ]

    # Check that plugin has desired effect
    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))
    assert_not_in("customVarValue9", result[0])


def test_data_type_defaults_to_passed_in_data_type():
    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2014-04-02"},
        "start_date": date(2013, 4, 1)
    }]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"],
    }
    data_type = "original"
    options = {}

    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))

    eq_(result[0]['dataType'], 'original')


def test_additional_fields():

    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02", "customVarValue9": "foo"},
        "start_date": date(2013, 4, 1),
    }]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date", "customVarValue9"],
    }
    data_type = "test"
    options = {
        "additionalFields": {"foo": "bar"},
    }

    # Check that foo is set on the output document
    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))
    assert_in("foo", result[0])
    assert_that(result[0]["foo"], equal_to("bar"))


def test_daily_repeat():

    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02", "customVarValue9": "foo"},
        "start_date": date(2013, 4, 1),
    }]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date", "customVarValue9"],
        "frequency": 'daily'
    }
    data_type = "test"
    options = {
    }

    # Check that foo is set on the output document
    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))
    assert_that(result[0]["timeSpan"], equal_to("day"))
    query['frequency'] = 'monthly'
    start, end = date(2013, 4, 1), date(2013, 4, 30)
    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))
    assert_that(result[0]["timeSpan"], equal_to("month"))


def test_data_type_can_be_overriden():
    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2014-04-02"},
        "start_date": date(2013, 4, 1)
    }]

    query = {
        "id": "ga:123",
        "metrics": ["visits"],
        "dimensions": ["date"],
    }
    data_type = "original"
    options = {"dataType": "overriden"}

    special_fields = list(ga_build_document_set(data))
    result = list(DataParser(data, options, query, data_type).get_data(
        special_fields
    ))

    eq_(result[0]['dataType'], 'overriden')
