# encoding: utf-8
from datetime import date
from hamcrest import(
    assert_that,
    is_,
    has_entries,
    has_item,
    equal_to,
    is_not,
    calling,
    raises)

from nose.tools import (
    eq_,
    assert_raises,
    assert_in,
    assert_not_in)

from performanceplatform.utils.data_parser import \
    build_document, build_document_set, \
    apply_key_mapping, map_multi_value_fields, \
    DataParser, get_string_for_data_id, value_id

from performanceplatform.collector.ga.core import \
    build_document_set as ga_build_document_set

from tests.performanceplatform.collector.ga import dt


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
    special_fields_for_doc['timeSpan'] = 'week'
    data = build_document(
        gapy_response, "weeklyvisits", special_fields_for_doc)

    assert_that(data, has_entries({
        "_id": "d2Vla2x5dmlzaXRzXzIwMTMwNDAxMDAwMDAwX3dlZWtfMjAxMy0wNC0wMg==",
        "humanId": 'weeklyvisits_20130401000000_week_2013-04-02',
        "dataType": "weeklyvisits",
        "_timestamp": dt(2013, 4, 1, 0, 0, 0, "UTC"),
        "date": "2013-04-02",
        "visits": 12345,
    }))


def test_get_string_for_data_id():
    assert_that(
        get_string_for_data_id(
            "a", dt(2012, 1, 1, 12, 0, 0, "UTC"), "week", ['one', 'two']),
        equal_to("a_20120101120000_week_one_two")
    )


def test_unicode_get_string_for_data_id():
    human = get_string_for_data_id(
        "a",
        dt(2012, 1, 1, 12, 0, 0, "UTC"),
        "week",
        ['one', u"© ☯ ☮"])

    assert_that(
        human.encode('utf-8'),
        is_('a_20120101120000_week_one_\xc2\xa9 \xe2\x98\xaf \xe2\x98\xae'))


def test_value_id_returns_base64_encoded_id():
    human = get_string_for_data_id(
        "a",
        dt(2012, 1, 1, 12, 0, 0, "UTC"),
        "week",
        ['one', u"© ☯ ☮"])

    (base64, new_human) = value_id(human)
    assert_that(base64,
                is_("YV8yMDEyMDEwMTEyMDAwMF93ZWVrX29uZV_CqSDimK8g4piu"))
    assert_that(
        new_human,
        human.encode('utf-8'))


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
    data_type = "test"
    options = {}

    result = list(DataParser(data, options, data_type).get_data())

    assert_in("customVarValue9", result[0])

    # Add the plugin
    options["plugins"] = [
        'RemoveKey("customVarValue9")',
        'ComputeIdFrom("date")',
    ]

    # Check that plugin has desired effect
    result = list(DataParser(data, options, data_type).get_data())
    assert_not_in("customVarValue9", result[0])


def test_data_type_defaults_to_passed_in_data_type():
    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2014-04-02"},
        "start_date": date(2013, 4, 1)
    }]

    data_type = "original"
    options = {}

    result = list(DataParser(data, options, data_type).get_data())

    eq_(result[0]['dataType'], 'original')


def test_additional_fields():

    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2013-04-02", "customVarValue9": "foo"},
        "start_date": date(2013, 4, 1),
    }]

    data_type = "test"
    options = {
        "additionalFields": {"foo": "bar"},
    }

    # Check that foo is set on the output document
    result = list(DataParser(data, options, data_type).get_data())
    assert_in("foo", result[0])
    assert_that(result[0]["foo"], equal_to("bar"))


def test_data_type_can_be_overriden():
    data = [{
        "metrics": {"visits": "12345"},
        "dimensions": {"date": "2014-04-02"},
        "start_date": date(2013, 4, 1)
    }]

    data_type = "original"
    options = {"dataType": "overriden"}

    result = list(DataParser(data, options, data_type).get_data())

    eq_(result[0]['dataType'], 'overriden')


def test_build_document_set_handles_big_numbers():
    results = [0] * 3142
    special_fields = [0] * 3142

    assert_that(
        calling(build_document_set).with_args(results, '', {}, special_fields),
        is_not(raises(ValueError)))
