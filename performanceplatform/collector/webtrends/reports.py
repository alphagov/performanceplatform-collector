from performanceplatform.utils import requests_with_backoff
from datetime import datetime, timedelta
import pytz
from performanceplatform.collector.webtrends.base import(
    BaseCollector, BaseParser)


class Collector(BaseCollector):
    def __init__(self, credentials, query, start_at, end_at):
        self.base_url = credentials['reports_url']
        self.report_id = query.pop('report_id')
        super(Collector, self).__init__(credentials, query, start_at, end_at)

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

    def build_parser(self, data_set_config, options):
        return Parser(options, data_set_config['data-type'])


class Parser(BaseParser):
    def __init__(self, options, data_type):
        self.row_type_name = options['row_type_name']
        super(Parser, self).__init__(options, data_type)

    def parse_item(self, item):
        initial_data = []
        special_fields = []
        for date, data_point in item.items():
            start_date = Parser.to_datetime(date)
            if data_point['SubRows']:
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


def main(credentials, data_set_config, query, options, start_at, end_at):
    collector = Collector(credentials, query, start_at, end_at)
    collector.collect_parse_and_push(data_set_config, options)
