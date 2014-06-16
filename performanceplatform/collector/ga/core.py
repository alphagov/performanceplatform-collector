import base64
import json
import logging
import re
from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from performanceplatform.utils import statsd

from gapy.client import from_private_key, from_secrets_file

from performanceplatform.collector.ga.datetimeutil \
    import to_datetime, period_range, to_utc


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


def send_data(data_set, documents):
    if len(documents) == 0:
        logging.info("No data returned with current configuration")
        return

    data_set.post(documents)


def _format(timestamp):
    return to_utc(timestamp).strftime("%Y%m%d%H%M%S")


def value_id(value):
    value_bytes = value.encode('utf-8')
    logging.debug(u"'{0}' ({1})".format(value, type(value)))
    return base64.urlsafe_b64encode(value_bytes), value_bytes


def data_id(data_type, timestamp, period, dimension_values):
    # `dimension_values` may be non-string python types and need to be coerced.
    values = map(unicode, dimension_values)
    slugs = [data_type, _format(timestamp), period] + values
    return value_id("_".join(slugs))


def map_one_to_one_fields(mapping, pairs):
    """
    >>> mapping = {'a': 'b'}
    >>> pairs = {'a': 1}
    >>> map_one_to_one_fields(mapping, pairs)
    {'b': 1}
    >>> mapping = {'a': ['b', 'a']}
    >>> map_one_to_one_fields(mapping, pairs)
    {'a': 1, 'b': 1}
    """
    mapped_pairs = dict()
    for key, value in pairs.items():
        if key in mapping:
            targets = mapping[key]
            if not isinstance(targets, list):
                targets = list(targets)
            for target in targets:
                mapped_pairs[target] = value
        else:
            mapped_pairs[key] = value

    return mapped_pairs


def map_multi_value_fields(mapping, pairs):
    multi_value_regexp = '^(.*)_(\d*)$'
    multi_value_delimiter = ':'
    mapped_pairs = {}

    for from_key, to_key in mapping.items():
        multi_value_matches = re.search(multi_value_regexp, from_key)
        if multi_value_matches:
            key = multi_value_matches.group(1)
            index = int(multi_value_matches.group(2))
            multi_value = pairs.get(key)
            if multi_value is None:
                continue

            values = multi_value.split(multi_value_delimiter)
            if index < len(values):
                mapped_pairs[to_key] = values[index]

    return mapped_pairs


def apply_key_mapping(mapping, pairs):
    logging.warn("{} -- {}".format(mapping, pairs))
    return dict(map_one_to_one_fields(mapping, pairs).items() +
                map_multi_value_fields(mapping, pairs).items())


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


def build_document(item, data_type,
                   mappings=None, idMapping=None, timespan='week'):
    if data_type is None:
        raise ValueError("Must provide a data type")
    if mappings is None:
        mappings = {}

    if idMapping is not None:
        if isinstance(idMapping, list):
            values_for_id = map(lambda d: item['dimensions'][d], idMapping)
            value_for_id = "".join(values_for_id)
        else:
            value_for_id = item['dimensions'][idMapping]

        (_id, human_id) = value_id(value_for_id)
    else:
        (_id, human_id) = data_id(
            data_type,
            to_datetime(item["start_date"]),
            timespan,
            item.get('dimensions', {}).values())

    base_properties = {
        "_id": _id,
        "_timestamp": to_datetime(item["start_date"]),
        "humanId": human_id,
        "timeSpan": timespan,
        "dataType": data_type
    }
    dimensions = apply_key_mapping(
        mappings,
        item.get("dimensions", {})).items()

    metrics = [(key, try_number(value))
               for key, value in item["metrics"].items()]
    return dict(base_properties.items() + dimensions + metrics)


def pretty_print(obj):
    return json.dumps(obj, indent=2)


def build_document_set(results, data_type, mappings, idMapping=None,
                       timespan='week'):
    return [build_document(item, data_type, mappings, idMapping,
                           timespan=timespan)
            for item in results]


def query_for_range(client, query, range_start, range_end):
    items = []
    frequency = query.get('frequency', 'weekly')

    for start, end in period_range(range_start, range_end, frequency):
        items.extend(query_ga(client, query, start, end))

    return items


def run_plugins(plugins_strings, results):

    last_plugin = plugins_strings[-1]
    if not last_plugin.startswith("ComputeIdFrom"):
        raise RuntimeError("Last plugin must be `ComputeIdFrom` for now. This "
                           "may be changed with further development if "
                           "necessary")

    # Import is here so that it is only required when "plugins" is specified
    from performanceplatform.collector.ga.plugins import load_plugins
    plugins = load_plugins(plugins_strings)

    # Plugins are designed so that their configuration is described in the
    # plugin string. At this point, `plugin` should be a closure which only
    # requires the documents to process.
    for plugin in plugins:
        results = plugin(results)

    return results


def query_documents_for(client, query, options,
                        data_type, start_date, end_date):
    results = query_for_range(client, query, start_date, end_date)

    data_type = options.get('dataType', data_type)
    mappings = options.get("mappings", {})
    idMapping = options.get("idMapping", None)

    frequency = query.get('frequency', 'weekly')
    frequency_to_timespan_mapping = {
        'daily': 'day',
        'weekly': 'week',
        'monthly': 'month',
    }
    timespan = frequency_to_timespan_mapping[frequency]

    docs = build_document_set(results, data_type, mappings, idMapping,
                              timespan=timespan)

    if "additionalFields" in options:
        additional_fields = options["additionalFields"]
        for doc in docs:
            doc.update(additional_fields)

    if "plugins" in options:
        docs = run_plugins(options["plugins"], docs)

    return docs
