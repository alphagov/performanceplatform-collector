from datetime import datetime, timedelta
import json
import logging

from performanceplatform.client import DataSet

from performanceplatform.collector.pingdom.core import Pingdom

_EARLIEST_DATE = datetime(2005, 1, 1)


def main(credentials, data_set_config, query, options, start_at, end_at):
    start_dt, end_dt = parse_time_range(start_at, end_at)

    pingdom = Pingdom(credentials)

    check_name = query['name']
    pingdom_stats = pingdom.stats(check_name, start_dt, end_dt)

    push_stats_to_data_set(
        pingdom_stats,
        check_name,
        data_set_config)


def parse_time_range(start_dt, end_dt):
    """
    Convert the start/end datetimes specified by the user, specifically:
    - truncate any minutes/seconds
    - for a missing end time, use start + 24 hours
    - for a missing start time, use end - 24 hours
    - for missing start and end, use the last 24 hours
    """
    now = datetime.now()

    if start_dt and not end_dt:
        end_dt = now

    elif end_dt and not start_dt:
        start_dt = _EARLIEST_DATE

    elif not start_dt and not end_dt:  # last 24 hours
        end_dt = now
        start_dt = end_dt - timedelta(days=1)

    return tuple(map(truncate_hour_fraction, (start_dt, end_dt)))


def push_stats_to_data_set(pingdom_stats, check_name, data_set_config):
    data_set = DataSet.from_config(data_set_config)
    data_set.post(
        [convert_from_pingdom_to_performanceplatform(thing, check_name) for
         thing in pingdom_stats])


def get_contents_as_json(path_to_file):
    with open(path_to_file) as file_to_load:
        logging.debug(path_to_file)
        return json.load(file_to_load)


def convert_from_pingdom_to_performanceplatform(pingdom_stats, name_of_check):
    timestamp = pingdom_stats['starttime'].isoformat()
    name_for_id = name_of_check.replace(' ', '_')
    return {
        '_id': "%s.%s" % (name_for_id, timestamp),
        '_timestamp': timestamp,
        'avgresponse': pingdom_stats['avgresponse'],
        'uptime': pingdom_stats['uptime'],
        'downtime': pingdom_stats['downtime'],
        'unmonitored': pingdom_stats['unmonitored'],
        'check': name_of_check
    }


def truncate_hour_fraction(a_datetime):
    return a_datetime.replace(minute=0, second=0, microsecond=0)
