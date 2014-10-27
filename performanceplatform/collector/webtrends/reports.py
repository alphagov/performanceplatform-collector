from performanceplatform.utils import requests_with_backoff
from performanceplatform.client import DataSet
from datetime import datetime, timedelta
import pytz
from performanceplatform.utils.data_parser import DataParser


class Collector(object):
    def __init__(self, credentials, query, start_at, end_at):
        self.start_at = start_at
        self.end_at = end_at
        self.user = credentials['user']
        self.password = credentials['password']
        self.base_url = credentials['reports_url']
        self.report_id = query.pop('report_id')
        self.query = query
        self.query_format = 'json'

    @classmethod
    def parse_date_for_query(cls, date):
        return date.strftime("%Ym%md%d")

    @classmethod
    def parse_standard_date_string_to_date(cls, date_string):
        return datetime.strptime(date_string, "%Y-%m-%d")

    @classmethod
    def date_range_for_webtrends(cls, start_at=None, end_at=None):
        """
        Get the day dates in between start and end formatted for query.
        This returns dates inclusive e.g. final day is (end_at, end_at+1 day)
        """
        if start_at and end_at:
            start_date = cls.parse_standard_date_string_to_date(
                start_at)
            end_date = cls.parse_standard_date_string_to_date(
                end_at)
            numdays = (end_date - start_date).days + 1
            end_dates = [(end_date + timedelta(1)) - timedelta(days=x)
                         for x in reversed(range(0, numdays))]
            start_dates = [end_date - timedelta(days=x)
                           for x in reversed(range(0, numdays))]
            date_range = []
            for i, date in enumerate(start_dates):
                date_range.append((
                    cls.parse_date_for_query(date),
                    cls.parse_date_for_query(end_dates[i])))
            return date_range
        else:
            return [("current_day-2", "current_day-1")]

    def _make_request(self, start_at_for_webtrends, end_at_for_webtrends):
        return requests_with_backoff.get(
            url="{base_url}{report_id}".format(
                base_url=self.base_url,
                report_id=self.report_id),
            auth=(self.user, self.password),
            params={
                'start_period': start_at_for_webtrends,
                'end_period': end_at_for_webtrends,
                'format': self.query_format
            }
        )

    def collect(self):
        data = []
        for start_at, end_at in self.get_date_range_for_webtrends():
            response = self._make_request(
                start_at,
                end_at)
            response.raise_for_status()
            data.append(response.json()["data"])
        return data

    def get_date_range_for_webtrends(self):
        return Collector.date_range_for_webtrends(self.start_at, self.end_at)

    def collect_parse_and_push(self, data_set_config, options):
        raw_json_data = self.collect()
        parsed_data = Parser(
            options, self.query, data_set_config['data-type']
        ).parse(raw_json_data)
        Pusher(data_set_config, options).push(parsed_data)


class Parser(object):
    def __init__(self, options, query, data_type):
        self.options = options
        self.row_type_name = options['row_type_name']
        self.query = query
        self.data_type = data_type

    def parse(self, data):
        base_items = []
        special_fields = []
        for item in data:
            res = self.parse_item(item)
            base_items += res[0]
            special_fields += res[1]
        return DataParser(
            base_items, self.options, self.query, self.data_type
        ).get_data(special_fields)

    def parse_item(self, item):
        initial_data = []
        special_fields = []
        for date, data_point in item.items():
            start_date = Parser.to_datetime(date)
            for row_type, row in data_point['SubRows'].items():
                initial_data.append({
                    'start_date': start_date,
                })
                special_fields.append(dict({
                    self.row_type_name: row_type
                }.items() + row['measures'].items()))
        return initial_data, special_fields

    @classmethod
    def to_datetime(cls, webtrends_formatted_date):
        start_of_period = webtrends_formatted_date.split("-")[0]
        return datetime.strptime(
            start_of_period, "%m/%d/%Y").replace(tzinfo=pytz.UTC)


class Pusher(object):
    def __init__(self, target_data_set_config, options):
        self.data_set_client = DataSet.from_config(target_data_set_config)
        self.chunk_size = options.get('chunk-size', 100)

    def push(self, data):
        self.data_set_client.post(
            data, chunk_size=self.chunk_size)


def main(credentials, data_set_config, query, options, start_at, end_at):
    collector = Collector(credentials, query, start_at, end_at)
    collector.collect_parse_and_push(data_set_config, options)
