from performanceplatform.collector.ga.plugins.remove_key \
    import RemoveKey


def test_RemoveKey():
    from nose.tools import assert_equal

    doc = {"a": None, "b": None}

    plugin = RemoveKey("b")
    (output_doc,) = plugin([doc])

    expected_doc = {"a": None}
    assert_equal(expected_doc, output_doc)
