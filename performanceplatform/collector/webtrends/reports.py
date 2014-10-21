from performanceplatform.utils import requests_with_backoff
from performanceplatform.client import DataSet
from datetime import datetime, time
import pytz

class Collector(object):
    def __init__(self, credentials, query, start_at, end_at):
        # CAN ONLY COLLECT UP TO DAILY?
        # period type doesn't work so only daily?
        # query should make up not be broken down - passed whole sale to params?
        print start_at
        print end_at
        self.start_at = start_at
        self.end_at = end_at
        self.user = credentials['user']
        self.password = credentials['password']
        self.base_url = credentials['reports_url']
        self.report_id = query.pop('report_id')
        if 'format' in query:
            self.query_format = query['format']
        else:
            self.query_format = 'json'

    @classmethod
    def parse_date_for_query(cls, date_string):
        date = datetime.strptime(date_string, "%Y-%m-%d")
        return date.strftime("%Ym%md%d")

    @classmethod
    def date_range_for_webtrends(cls, start_at=None, end_at=None):
        if start_at and end_at:
            return [("current_day-2", "current_day-1")]
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

    def start_at_for_webtrends(self):
        if self.start_at:
            return self.start_at
        #figure out formats here, how to parse standard format for this to their format?
        # default as most of these reports are generated daily
        return "current_day-2"

    def end_at_for_webtrends(self):
        if self.end_at:
            return self.end_at
        # default as most of these reports are generated daily
        return "current_day-1"

    def get_date_range_for_webtrends(self):
        #don't do it if hourly - only daily
        return Collector.date_range_for_webtrends()

    def collect_parse_and_push(self, data_set_config, options):
        raw_json_data = self.collect()
        parsed_data = Parser(options).parse(raw_json_data)
        Pusher(data_set_config, options).push(parsed_data)


class Parser(object):
    # loads common with ga here
    def __init__(self, options):
        self.options = options

    def parse(self, data):
        return {}

    @classmethod
    def to_datetime(cls, webtrends_formatted_date):
        start_of_period = webtrends_formatted_date.split("-")[0]
        return datetime.strptime(start_of_period, "%m/%d/%Y").replace(tzinfo=pytz.UTC)


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
