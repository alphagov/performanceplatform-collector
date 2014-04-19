from backdrop.collector.ga.plugins.department \
    import ComputeDepartmentKey, try_get_department


def test_try_get_department():
    from nose.tools import assert_equal
    assert_equal(try_get_department("<D1>"), "attorney-generals-office")
    assert_equal(try_get_department("<D1><foo>"), "attorney-generals-office")
    assert_equal(try_get_department("<foo>"), "<foo>")


def test_mapping():
    from nose.tools import assert_equal, assert_in

    plugin = ComputeDepartmentKey("key_name")
    documents = [{"key_name": "<D10>"}]
    (transformed_document, ) = plugin(documents)

    assert_in("department", transformed_document)
    assert_equal(transformed_document["department"],
                 "department-for-work-pensions")


def test_fail_if_no_key_name_in_document():
    from nose.tools import assert_raises

    plugin = ComputeDepartmentKey("key_name")
    documents = [{"foo": "<D10>"}]

    with assert_raises(AssertionError):
        plugin(documents)


def test_unknown_department_code():
    from nose.tools import assert_equal

    plugin = ComputeDepartmentKey("key_name")
    documents = [{"key_name": "<DTHISDOESNOTEXIST>"}]
    (transformed_document, ) = plugin(documents)

    assert_equal(transformed_document["department"], "<DTHISDOESNOTEXIST>")


def test_takes_first_code():
    from nose.tools import assert_equal

    plugin = ComputeDepartmentKey("key_name")
    documents = [{"key_name": "<D10><D9>"}]
    (transformed_document, ) = plugin(documents)

    assert_equal(transformed_document["department"],
                 "department-for-work-pensions")
