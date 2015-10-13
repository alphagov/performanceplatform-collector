import base64
from datetime import date, timedelta
from performanceplatform.collector.ga.lib.helper import create_client

from performanceplatform.client import DataSet
from performanceplatform.utils.datetimeutil \
    import to_datetime

ga_date_keys = ['day', 'month', 'year']


def parse_query(query):
    if 'metric' not in query or not query['metric']:
        raise Exception('Metric required')
    else:
        if 'dimensions' in query:
            query['dimensions'].extend(ga_date_keys)
        else:
            query['dimensions'] = ga_date_keys

    return query


def get_date():
    inclusive_end = date.today()
    exclusive_end = inclusive_end - timedelta(days=1)
    middle = inclusive_end - timedelta(weeks=1)
    start = inclusive_end - timedelta(weeks=2)
    return (start, middle, exclusive_end)


def assign_day_to_week(day, month, year, dates):

    start, middle, end = dates

    d = date(int(year), int(month), int(day))

    return 2 if (d >= middle) else 1


def get_trends(data):

    for key in data:
        numerator = data[key]['week2'] - data[key]['week1']
        denominator = data[key]['week1']

        percent_change = ((float(numerator) / float(denominator)) * 100)
        data[key]['percent_change'] = percent_change

    return data


def sum_data(data, metric, collapse_key, dates, floor):

    collapsed = {}

    for row in data:

        dimensions = row['dimensions']
        k = dimensions[collapse_key]

        if k not in collapsed:
            d = {}
            for dim in dimensions:
                if dim not in ga_date_keys:
                    d[dim] = dimensions[dim]
            d['week1'] = 0
            d['week2'] = 0
            d['_timestamp'] = to_datetime(dates[0])
            collapsed[k] = d

        date_list = [dimensions['day'],
                     dimensions['month'], dimensions['year']]
        if '(other)' in date_list:
            continue
        week = 'week%d' % assign_day_to_week(dimensions['day'],
                                             dimensions['month'],
                                             dimensions['year'], dates)

        # Use the shortest common path.
        if dimensions['pagePath'] in collapsed[k]['pagePath']:
            collapsed[k]['pagePath'] = dimensions['pagePath']

        collapsed[k][week] += int(row['metrics'][metric])

    for key in collapsed:
        for week in ['week1', 'week2']:
            if collapsed[key][week] < floor:
                collapsed[key][week] = floor

    return collapsed


def encode_id(id):
    id_bytes = id.encode('utf-8')
    return base64.urlsafe_b64encode(id_bytes)


def flatten_data_and_assign_ids(data):

    flattened = []

    for key in data:
        data[key]['_id'] = encode_id(data[key]['pagePath'])
        flattened.append(data[key])

    return flattened


def query_ga(client, end, query, start):
    ga_query = parse_query(query)

    return client.query.get(
        ga_query['id'],
        start,
        end,
        [ga_query['metric']],
        ga_query['dimensions'],
        ga_query['filters'] if 'filters' in ga_query else None,
        None,
        None,
        ga_query.get('segment')
    )


def main(credentials, data_set_config, query, options, start_at, end_at):

    credentials = credentials
    client = create_client(credentials)

    collapse_key = "pageTitle"

    (start, middle, end) = get_date()

    data = query_ga(client, end, query, start)

    collapsed_data = sum_data(data, query['metric'], collapse_key,
                              (start, middle, end), 500)
    trended_data = get_trends(collapsed_data)
    flattened_data = flatten_data_and_assign_ids(trended_data)

    data_set = DataSet.from_config(data_set_config)

    data_set.empty_data_set()
    data_set.post(flattened_data)
