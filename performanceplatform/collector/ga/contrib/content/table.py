from performanceplatform.collector.ga.plugins.department import \
    try_get_department

from performanceplatform.collector.ga import \
    create_client, query_documents_for, send_data

from performanceplatform.client import DataSet


def main(credentials, data_set_config, query, options, start_at, end_at):
    client = create_client(credentials)

    assert "filtersets" in options, "`filtersets` must be specified"

    documents = []

    original_filters = query.get("filters", [])

    def insert_ga(filters):
        # Insert ga: on everything but the first.
        def gen():
            for i, f in enumerate(filters):
                if i == 0:
                    yield f
                else:
                    yield "ga:" + f

        return list(gen())

    for filters in options["filtersets"]:
        all_filters = insert_ga(original_filters + filters)
        query["filters"] = [";".join(all_filters)]

        for f in filters:
            if f.startswith("customVarValue9=~^"):
                department = try_get_department(f[len("customVarValue9=~^"):])
                break
        else:
            # If no matches, raise a RuntimeError
            raise RuntimeError("department not found in filters, expected "
                               "filter expression beginning "
                               "'customVarValue9=~^'.")

        options.setdefault("additionalFields", {}).update(
            {"department": department})

        ds = query_documents_for(
            client, query, options, data_set_config['data-type'],
            start_at, end_at
        )

        documents.extend(ds)

    send_data(DataSet.from_config(data_set_config), documents)
