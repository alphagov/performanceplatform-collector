import json
import logging
from performanceplatform.utils.data_parser import DataParser

from performanceplatform.utils.datetimeutil \
    import period_range


def query_ga(client, config, start_date, end_date):
    logging.info("Querying GA for data in the period: %s - %s"
                 % (str(start_date), str(end_date)))

    # If maxResults is 0, don't include it in the query.
    # Same for a sort == [].

    maxResults = config.get("maxResults", None)
    if maxResults == 0:
        maxResults = None

    sort = config.get("sort", None)
    if sort == []:
        sort = None

    return client.query.get(
        config["id"].replace("ga:", ""),
        start_date,
        end_date,
        config["metrics"],
        config.get("dimensions"),
        config.get("filters"),
        maxResults,
        sort,
        config.get("segment")
    )


def try_number(value):
    """
    Attempt to cast the string `value` to an int, and failing that, a float,
    failing that, raise a ValueError.
    """

    for cast_function in [int, float]:
        try:
            return cast_function(value)
        except ValueError:
            pass

    raise ValueError("Unable to use value as int or float: {0!r}"
                     .format(value))


def convert_durations(metric):
    """
    Convert session duration metrics from seconds to milliseconds.
    """
    if metric[0] == 'avgSessionDuration' and metric[1]:
        new_metric = (metric[0], metric[1] * 1000)
    else:
        new_metric = metric
    return new_metric


def build_document(item):
    metrics = [(key, try_number(value))
               for key, value in item["metrics"].items()]
    metrics = [convert_durations(metric) for metric in metrics]

    return dict(metrics)


def pretty_print(obj):
    return json.dumps(obj, indent=2)


def build_document_set(results):
    return (build_document(item)
            for item in results)


def query_for_range(client, query, range_start, range_end):
    frequency = query.get('frequency', 'weekly')

    for start, end in period_range(range_start, range_end, frequency):
        for record in query_ga(client, query, start, end):
            yield record


def query_documents_for(client, query, options,
                        data_type, start_date, end_date):
    results = query_for_range(client, query, start_date, end_date)

    results = list(results)
    frequency = query.get('frequency', 'weekly')
    special_fields = add_timeSpan(frequency, build_document_set(results))
    return DataParser(results, options, data_type).get_data(
        special_fields
    )


def add_timeSpan(frequency, special_fields):
    frequency_to_timespan_mapping = {
        'daily': 'day',
        'weekly': 'week',
        'monthly': 'month',
    }
    timespan = frequency_to_timespan_mapping[frequency]
    return [dict(item.items() + [('timeSpan', timespan)])
            for item in special_fields]
