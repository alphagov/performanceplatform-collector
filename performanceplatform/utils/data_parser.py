from performanceplatform.collector.ga.datetimeutil \
    import to_datetime, to_utc
import logging
import base64
import re


class DataParser(object):
    def __init__(self, data, options, query, data_type):
        self.data = [item for item in data]
        if "plugins" in options:
            self.plugins = options["plugins"]
        else:
            self.plugins = None
        self.data_type = options.get('dataType', data_type)
        self.mappings = options.get("mappings", {})
        self.idMapping = options.get("idMapping", None)
        self.additionalFields = options.get('additionalFields', {})

        frequency = query.get('frequency', 'weekly')
        frequency_to_timespan_mapping = {
            'daily': 'day',
            'weekly': 'week',
            'monthly': 'month',
        }
        self.timespan = frequency_to_timespan_mapping[frequency]

    def get_data(self, special_fields):
        docs = build_document_set(self.data, self.data_type, self.mappings,
                                  special_fields,
                                  self.idMapping, timespan=self.timespan,
                                  additionalFields=self.additionalFields)

        if self.plugins:
            docs = run_plugins(self.plugins, list(docs))

        return docs


def build_document_set(results, data_type, mappings, special_fields,
                       idMapping=None,
                       timespan='week',
                       additionalFields={}):
    return (build_document(item, data_type, special_fields[i], mappings,
                           idMapping, timespan=timespan,
                           additionalFields=additionalFields)
            for i, item in enumerate(results))


def build_document(item, data_type, special_fields,
                   mappings=None, idMapping=None,
                   timespan='week', additionalFields={}):
    if data_type is None:
        raise ValueError("Must provide a data type")
    if mappings is None:
        mappings = {}

    base_properties = {
        "_timestamp": to_datetime(item["start_date"]),
        "timeSpan": timespan,
        "dataType": data_type
    }

    doc = dict(base_properties.items() +
               additionalFields.items() +
               item.get("dimensions", {}).items() +
               special_fields.items())
    doc = apply_key_mapping(mappings, doc)

    if idMapping is not None:
        if isinstance(idMapping, list):
            values_for_id = map(lambda d: unicode(doc[d]), idMapping)
            value_for_id = "".join(values_for_id)
        else:
            value_for_id = doc[idMapping]

        (_id, human_id) = value_id(value_for_id)
    else:
        (_id, human_id) = data_id(
            data_type,
            to_datetime(item["start_date"]),
            timespan,
            item.get('dimensions', {}).values())

    doc['humanId'] = human_id
    doc['_id'] = _id

    return doc


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


def apply_key_mapping(mapping, pairs):
    return dict(map_one_to_one_fields(mapping, pairs).items() +
                map_multi_value_fields(mapping, pairs).items())


def map_one_to_one_fields(mapping, pairs):
    return dict((mapping.get(key, key), value) for key, value in pairs.items())


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


def data_id(data_type, timestamp, period, dimension_values):
    # `dimension_values` may be non-string python types and need to be coerced.
    values = map(unicode, dimension_values)
    slugs = [data_type, _format(timestamp), period] + values
    return value_id("_".join(slugs))


def _format(timestamp):
    return to_utc(timestamp).strftime("%Y%m%d%H%M%S")


def value_id(value):
    value_bytes = value.encode('utf-8')
    logging.debug(u"'{0}' ({1})".format(value, type(value)))
    return base64.urlsafe_b64encode(value_bytes), value_bytes
