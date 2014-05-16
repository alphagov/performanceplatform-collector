from performanceplatform.collector.ga.plugins.compute_id \
    import ComputeIdFrom, value_id


def test_ComputeIdFrom():
    from nose.tools import assert_in, assert_equal

    documents = [
        {"a": 1, "b": 2, "c": 3}
    ]

    plugin = ComputeIdFrom("a", "b")

    documents = plugin(documents)

    (document,) = documents

    assert_in("a", document)
    assert_in("b", document)

    assert_in("_id", document)
    assert_in("humanId", document)

    from pprint import pprint
    pprint(document)

    _id, humanId = value_id("{0}_{1}".format(document['a'], document['b']))
    assert_equal(_id, document['_id'])
    assert_equal(humanId, document['humanId'])
