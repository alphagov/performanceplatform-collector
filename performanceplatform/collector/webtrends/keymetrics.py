from performanceplatform.utils import requests_with_backoff
from datetime import datetime
import pytz
from performanceplatform.collector.webtrends.base import(
    BaseCollector, BaseParser)


class Collector(BaseCollector):
    def __init__(self, credentials, query, start_at, end_at):
        self.base_url = credentials['keymetrics_url']
        super(Collector, self).__init__(credentials, query, start_at, end_at)

    @classmethod
    def parse_date_for_query(cls, date):
        return date.strftime("%Ym%md%d")

    @classmethod
    def parse_standard_date_string_to_date(cls, date_string):
        if type(date_string) == datetime:
            return date_string
        return datetime.strptime(date_string, "%Y-%m-%d")

    @classmethod
    def date_range_for_webtrends(cls, start_at=None, end_at=None):
        """
        Get the start and end formatted for query
        or the last hour if none specified.
        """
        if start_at and end_at:
            start_date = cls.parse_standard_date_string_to_date(
                start_at)
            end_date = cls.parse_standard_date_string_to_date(
                end_at)
            return [(
                cls.parse_date_for_query(start_date),
                cls.parse_date_for_query(end_date))]
        else:
            return [("current_hour-1", "current_hour-1")]

    def _make_request(self, start_at_for_webtrends, end_at_for_webtrends):
        return requests_with_backoff.get(
            url="{base_url}".format(
                base_url=self.base_url),
            auth=(self.user, self.password),
            params={
                'start_period': start_at_for_webtrends,
                'end_period': end_at_for_webtrends,
                'format': self.query_format,
                "userealtime": True
            }
        )

    def get_date_range_for_webtrends(self):
        return Collector.date_range_for_webtrends(self.start_at, self.end_at)

    def build_parser(self, data_set_config, options):
        return Parser(options, data_set_config['data-type'])


class Parser(BaseParser):
    def parse_item(self, item):
        initial_data = []
        special_fields = []
        for key, value in item.items():
            if value['SubRows']:
                row_items = value['SubRows'].items()
                for date, row in row_items:
                    start_date = Parser.to_datetime(date)
                    if row:
                        initial_data.append({
                            'start_date': start_date,
                        })
                        special_fields.append(row['measures'])
        return initial_data, special_fields

    @classmethod
    def to_datetime(cls, webtrends_formatted_date):
        return datetime.strptime(
            webtrends_formatted_date, "%m/%d/%Y %H").replace(tzinfo=pytz.UTC)


def main(credentials, data_set_config, query, options, start_at, end_at):
    collector = Collector(credentials, query, start_at, end_at)
    collector.collect_parse_and_push(data_set_config, options)
