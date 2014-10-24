from performanceplatform.utils.datetimeutil \
    import to_datetime, to_utc
import logging
import base64
import re


class DataParser(object):
    def __init__(self, data, options, query, data_type):
        # it would be nice to have some json schemas or something to validate
        # for now here are some docs
        """
        data: Can be any array of dicts
        options: {
          mappings: "a dict: converts keys matching key of the mappings dict
              to the value in the mapping dict",
          additionalFields: "a dict:
              key value pairs will be added into the returned data",
          idMappings: "a list of keys or single key string: for each key
              the corresponding values will be concatenated in order to create
              a unique _human_id field on the returned data and base64
              encoded in order to create an _id field. If no idMappings
              are provided then the item start_date and any avaiable
              'dimensions' will be used instead",
          dataType: "a value to be set on a dataType attribute.
              Overrides the data_type argument"
          plugins: "a list of string respresentations of python classes being
              instantiated. e.g. ComputeIdFrom('_timestamp', '_timespan').
              To be run on the passed in data to mutate it.
              See performanceplatform.collector.ga.plugins for more details"
        }
        query: {
          frequency: "a string - either daily, weekly or monthly, to be
              converted to a timespan - either day month or week that is
              then set as a timespan attribute on the returned data"
        }
        data_type: "a string - the data_type to be set as a data_type
            attribute on the returned data unless it is overridden in options"
        """
        self.data = list(data)
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
        """
        special_fields: "a dict of data specific to collector type
            that should be added to the data returned by the parser.
            This will also be and operated on by idMappings, mappings and
            plugins"

        This method loops through the data provided to the instance.
        For each item it will return a dict of the format:
        {
            "_timestamp": "the item start_date",
            "timeSpan": "self.timespan for the instance",
            "dataType": "self.data_type for the instance",
            ...
            "any additional fields": "from self.additionalFields",
            "any special_fields": "from special fields argument",
            "any item dimensions": "from item.dimensions",
            ...
            mappings changing keys in this dict from self.mappings
               are then applied on the above
            ...
            "_human_id": "derived from either the values corresponding to
               idMappings concatenated or the data_type, item.start_date,
               timespan and item.dimensions values if any concatenated"
            "_id": "derived from either the values corresponding to
               idMappings concatenated or the data_type, item.start_date,
               timespan and item.dimensions values if any concatenated and
               then base64 encoded"
        }
        """
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
    if len(results) is not len(special_fields):
        raise ValueError(
            "There must be same number of special fields as results")
    return (build_document(item, data_type, special_fields[i], mappings,
                           idMapping, timespan=timespan,
                           additionalFields=additionalFields)
            for i, item in enumerate(results))


def build_document(item, data_type, special_fields={},
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
