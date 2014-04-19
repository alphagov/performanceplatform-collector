from backdrop.collector.ga.plugins.aggregate \
    import make_aggregate, aggregate_count, aggregate_rate, AggregateKey


def test_make_aggregate_sum():
    """
    test_make_aggregate_sum()

    Straight test that summation works over the field specified in
    aggregate_count(keyname).
    """
    from nose.tools import assert_equal

    doc1 = {"a": 2, "b": 2, "c": 2, "visits": 201}
    doc2 = {"a": 2, "b": 2, "c": 2, "visits": 103}
    docs = [doc1, doc2]

    aggregate_doc = make_aggregate(docs, [aggregate_count("visits")])
    expected_aggregate = {"a": 2, "b": 2, "c": 2, "visits": 304}
    assert_equal(aggregate_doc, expected_aggregate)


def test_make_aggregate_rate():
    """
    test_make_aggregate_rate()

    Test that aggregation works when there are two fields summed over, and
    that rate aggregation works correctly. It isn't easy to test independently
    because one must aggregate over the two fields to get the correct result.
    """
    from nose.tools import assert_equal

    doc1 = {"a": 2, "b": 2, "c": 2, "visits": 100, "rate": 0.25}
    doc2 = {"a": 2, "b": 2, "c": 2, "visits": 100, "rate": 0.75}
    docs = [doc1, doc2]

    aggregate_doc = make_aggregate(docs, [aggregate_count("visits"),
                                          aggregate_rate("rate", "visits")])
    expected_aggregate = {
        "a": 2, "b": 2, "c": 2,
        "visits": 200,
        "rate": (0.25 * 100 + 0.75 * 100) / (100 + 100)}

    assert_equal(aggregate_doc, expected_aggregate)


def test_AggregateKeyPlugin():
    """
    test_AggregateKeyPlugin()

    Test that the AggregateKey class behaves as a plugin correctly.
    """
    from nose.tools import assert_equal

    doc1 = {"a": 2, "b": 2, "c": 2, "visits": 100, "rate": 0.25}
    doc2 = {"a": 2, "b": 2, "c": 2, "visits": 100, "rate": 0.75}
    docs = [doc1, doc2]

    plugin = AggregateKey(aggregate_count("visits"),
                          aggregate_rate("rate", "visits"))

    output_docs = plugin(docs)

    expected_aggregate = {
        "a": 2, "b": 2, "c": 2,
        "visits": 200,
        "rate": (0.25 * 100 + 0.75 * 100) / (100 + 100)}

    assert_equal(output_docs, [expected_aggregate])
