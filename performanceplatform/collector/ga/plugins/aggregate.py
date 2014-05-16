from __future__ import division

from itertools import groupby


class AggregateKey(object):

    """
    Given a set of documents, find all records with equal "key" where the "key"
    is all values which are not being aggregated over.

    For example:

        doc1 = {"foo": "bar", "count": 2}
        doc2 = {"foo": "bar", "count": 2}

        plugin = AggregateKey(aggregate_count("count"))

        plugin([doc1, doc2]) => {"foo": "bar", "count": 4}

    Aggregate rates are also supported through
    `aggregate_rate(rate_key, count_key)`.
    """

    def __init__(self, *aggregations):
        self.aggregations = aggregations

    def __call__(self, documents):
        first = documents[0]

        aggregate_keys = [k for k, _ in self.aggregations]
        groupkeys = set(first) - set(aggregate_keys)

        def key(doc):
            return tuple(doc[key] for key in groupkeys)

        return [make_aggregate(grouped, self.aggregations)
                for grouped in group(documents, key)]


def group(iterable, key):
    """
    groupby which sorts the input, discards the key and returns the output
    as a sequence of lists.
    """
    for _, grouped in groupby(sorted(iterable, key=key), key=key):
        yield list(grouped)


def aggregate_count(keyname):
    """
    Straightforward sum of the given keyname.
    """
    def inner(docs):
        return sum(doc[keyname] for doc in docs)

    return keyname, inner


def aggregate_rate(rate_key, count_key):
    """
    Compute an aggregate rate for `rate_key` weighted according to
    `count_rate`.
    """
    def inner(docs):
        total = sum(doc[count_key] for doc in docs)
        weighted_total = sum(doc[rate_key] * doc[count_key] for doc in docs)
        total_rate = weighted_total / total
        return total_rate

    return rate_key, inner


def make_aggregate(docs, aggregations):
    """
    Given `docs` and `aggregations` return a single document with the
    aggregations applied.
    """
    new_doc = dict(docs[0])

    for keyname, aggregation_function in aggregations:
        new_doc[keyname] = aggregation_function(docs)

    return new_doc


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
