import re
from datetime import datetime
import pytz

from performanceplatform.collector.piwik.base import BaseFetcher
from performanceplatform.utils.data_pusher import Pusher
from performanceplatform.utils.data_parser import DataParser

FREQUENCY_TO_PERIOD_MAPPING = {
    'daily': 'day',
    'weekly': 'week',
    'monthly': 'month',
}


class Fetcher(BaseFetcher):
    def __init__(self, credentials, query, start_at, end_at):
        super(Fetcher, self).__init__(credentials, query)
        self.period = FREQUENCY_TO_PERIOD_MAPPING[
            query.get('frequency', 'weekly')]
        self.date = Fetcher.set_piwik_date(start_at, end_at)
        self.api_method_arguments = query.get('api_method_arguments', None)

    @staticmethod
    def set_piwik_date(start_at, end_at):
        if start_at and end_at:
            date = "{start},{end}".format(
                start=start_at.strftime("%Y-%m-%d"),
                end=end_at.strftime("%Y-%m-%d"))
        else:
            date = "previous1"
        return date

    def _request_params(self):
        params = super(Fetcher, self)._request_params()
        params.update({
            'period': self.period,
            'date': self.date})
        if self.api_method_arguments:
            params.update(self.api_method_arguments)
        return params


class Parser():
    def __init__(self, query, options, data_type):
        self.options = options
        self.data_type = data_type
        self.period = FREQUENCY_TO_PERIOD_MAPPING[
            query.get('frequency', 'weekly')]

    def parse(self, data):
        base_items = []
        special_fields = []
        for date_key, data_points in data.items():
            if type(data_points) == dict:
                data_points = [data_points]
            data = self._parse_item(date_key, data_points)
            base_items += data[0]
            special_fields += data[1]
        return DataParser(
            base_items, self.options, self.data_type
        ).get_data(special_fields)

    def _parse_item(self, date_key, data_points):
        base_items = []
        special_fields = []
        for data_point in data_points:
            start_date = Parser.to_datetime(date_key)
            base_items.append({'start_date': start_date})
            mapped_fields = {
                v: data_point[k] for k, v in self.options['mappings'].items()}
            mapped_fields['timeSpan'] = self.period
            special_fields.append(mapped_fields)
        return base_items, special_fields

    @staticmethod
    def to_datetime(date_key):
        '''
        Extract the first date from 'key' matching YYYY-MM-DD
        or YYYY-MM, and convert to datetime.
        '''
        match = re.search(r'\d{4}-\d{2}(-\d{2})?', date_key)
        formatter = '%Y-%m'
        if len(match.group()) == 10:
            formatter += '-%d'
        return datetime.strptime(
            match.group(), formatter).replace(tzinfo=pytz.UTC)


def main(credentials, data_set_config, query, options, start_at, end_at):
    data_type = data_set_config['data-type']

    data = Fetcher(credentials, query, start_at, end_at).fetch()

    parsed_data = Parser(query, options, data_type).parse(data)

    Pusher(data_set_config, options).push(parsed_data)
