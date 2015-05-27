import re
from performanceplatform.collector.ga.plugins.department import \
    try_get_department

from performanceplatform.collector.ga import \
    create_client, query_documents_for

from performanceplatform.utils.data_pusher import Pusher


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

        options.setdefault("additionalFields", {}).update(
            {"department": get_department(filters)})

        ds = query_documents_for(
            client, query, options, data_set_config['data-type'],
            start_at, end_at
        )

        documents.extend(ds)

    Pusher(data_set_config, options).push(documents)


def get_department(filters):
    for f in filters:
        try:
            filter_key, filter_value = re.split('=~\^', f)
            return try_get_department(filter_value)
        except:
            raise ValueError("department not found in filters, expected "
                             "filter expression expression containing "
                             "'=~^' e.g. 'Organisation=~^<A1>'.")
    else:
        # If no matches, raise a RuntimeError
        raise RuntimeError("department not found in filters, expected "
                           "filter expression beginning "
                           "'customVarValue9=~^'.")
