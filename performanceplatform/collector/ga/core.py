import json
import logging
from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from performanceplatform.utils import statsd
from performanceplatform.utils.data_parser import DataParser

from gapy.client import from_private_key, from_secrets_file

from performanceplatform.utils.datetimeutil \
    import period_range


def create_client(credentials):
    if "CLIENT_SECRETS" in credentials:
        return from_secrets_file(
            credentials['CLIENT_SECRETS'],
            storage_path=credentials['STORAGE_PATH'],
            http_client=HttpWithBackoff(),
            ga_hook=track_ga_api_usage,
        )
    else:
        return from_private_key(
            credentials['ACCOUNT_NAME'],
            private_key_path=credentials['PRIVATE_KEY'],
            storage_path=credentials['STORAGE_PATH'],
            http_client=HttpWithBackoff(),
            ga_hook=track_ga_api_usage,
        )


def track_ga_api_usage(kwargs):
    statsd.incr('ga.core.{}.count'.format(kwargs['ids'].replace(':', '')))


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
    special_fields = list(build_document_set(results))
    return DataParser(results, options, query, data_type).get_data(
        special_fields
    )
