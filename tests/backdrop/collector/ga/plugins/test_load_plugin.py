from backdrop.collector.ga.plugins.load_plugin \
    import load_plugin

from backdrop.collector.ga.plugins \
    import AggregateKey, ComputeDepartmentKey


def test_load_plugin_trivial():
    from nose.tools import assert_equal

    assert_equal(1, load_plugin("1"))


def test_load_plugin_compute_department_key():
    from nose.tools import assert_is_instance

    plugin = load_plugin('ComputeDepartmentKey("customVarValue9")')
    assert_is_instance(plugin, ComputeDepartmentKey)


def test_load_plugin_compute_aggregate_key():
    from nose.tools import assert_is_instance

    plugin = load_plugin('AggregateKey(aggregate_count("visits"),'
                         '             aggregate_rate("rate", "visits"))')
    assert_is_instance(plugin, AggregateKey)
