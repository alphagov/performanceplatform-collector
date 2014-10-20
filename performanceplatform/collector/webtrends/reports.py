from performanceplatform.utils import requests_with_backoff
from performanceplatform.client import DataSet

class Collector(object):
    def __init__(self, credentials, query, start_at, end_at):
        # query should make up not be broken down - passed whole sale to params?
        self.start_at = start_at
        self.end_at = end_at
        self.user = credentials['user']
        self.password = credentials['password']
        self.base_url = credentials['reports_url']
        self.report_id = query.pop('report_id')
        if 'format' in query:
            self.format = query['format']

    def collect(self):
        response = requests_with_backoff.get(
            url="{base_url}{report_id}".format(
              base_url=self.base_url,
              report_id=self.report_id),
            auth=(self.user, self.password),
            params={
                'start_period': self.start_at_for_webtrends(),
                'end_period': self.end_at_for_webtrends(),
                'format': self.format()
            }
        )
        response.raise_for_status()
        response_json = response.json()
        return response_json

    def start_at_for_webtrends(self):
        if self.start_at:
            self.start_at
        #figure out formats here, how to parse standard format for this to their format?
        # default as most of these reports are generated hourly
        return "current_day-2"

    def end_at_for_webtrends(self):
        if self.end_at:
            return self.end_at
        # default as most of these reports are generated hourly
        return "current_day-1"

    def format(self):
        if self.format:
          return self.format
        # csv format is possible but the way it's formatted is somewhat broken
        # - quoted things are consistent
        # we'll use json for now
        return "json"

    def collect_parse_and_push(self, data_set_config, options):
        raw_json_data = self.collect()
        parsed_data = Parser(options).parse(raw_json_data)
        Pusher(data_set_config, options).push(parsed_data)


class Parser(object):
    def __init__(self, options):
        self.options = options

    def parse(self, data):
        return {}


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
