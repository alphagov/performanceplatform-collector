from performanceplatform.utils.data_parser import DataParser
from performanceplatform.utils.data_pusher import Pusher
from datetime import datetime


class BaseCollector(object):
    def __init__(self, credentials, query, start_at, end_at):
        self.start_at = start_at
        self.end_at = end_at
        self.user = credentials['user']
        self.password = credentials['password']
        self.query_format = 'json'

    @classmethod
    def parse_date_for_query(cls, date):
        return date.strftime("%Ym%md%d")

    @classmethod
    def parse_standard_date_string_to_date(cls, date_string):
        if type(date_string) == datetime:
            return date_string
        return datetime.strptime(date_string, "%Y-%m-%d")

    def collect(self):
        data = []
        for start_at, end_at in self.get_date_range_for_webtrends():
            response = self._make_request(
                start_at,
                end_at)
            response.raise_for_status()
            data.append(response.json()["data"])
        return data

    def collect_parse_and_push(self, data_set_config, options):
        raw_json_data = self.collect()
        parsed_data = self.build_parser(
            data_set_config, options).parse(raw_json_data)
        Pusher(data_set_config, options).push(parsed_data)

    def get_date_range_for_webtrends(self):
        return self.__class__.date_range_for_webtrends(
            self.start_at, self.end_at)


class BaseParser(object):
    def __init__(self, options, data_type):
        self.options = options
        self.data_type = data_type

    def parse(self, data):
        base_items = []
        special_fields = []
        for item in data:
            res = self.parse_item(item)
            base_items += res[0]
            special_fields += res[1]
        return DataParser(
            base_items, self.options, self.data_type
        ).get_data(special_fields)
