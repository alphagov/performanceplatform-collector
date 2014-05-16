from performanceplatform.collector.ga.plugins.rank \
    import ComputeRank


def test_rank():
    plugin = ComputeRank("rank")

    docs = [{}, {}]

    result = plugin(docs)

    doc1, doc2 = result

    assert doc1["rank"] == 1
    assert doc2["rank"] == 2
