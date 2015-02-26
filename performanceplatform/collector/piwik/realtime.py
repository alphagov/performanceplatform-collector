import sys
from datetime import datetime
import pytz
from performanceplatform.collector.piwik.base import BaseFetcher
from performanceplatform.utils.data_pusher import Pusher


class Fetcher(BaseFetcher):
    def __init__(self, credentials, query):
        super(Fetcher, self).__init__(credentials, query)
        self.last_minutes = query.get('minutes', 2)

    def _request_params(self):
        params = super(Fetcher, self)._request_params()
        params.update({'lastMinutes': self.last_minutes})
        return params


class Parser():
    def _timestamp(self):
        timezone = pytz.UTC
        timestamp = datetime.now(timezone).replace(microsecond=0)
        return timestamp.isoformat()

    def parse(self, data):
        timestamp = self._timestamp()
        visitor_count = int(data[0]["visitors"])
        return [{
            "_timestamp": timestamp,
            "_id": timestamp,
            "unique_visitors": visitor_count,
            "for_url": ""}]


def main(credentials, data_set_config, query, options, start_at, end_at):
    if start_at or end_at:
        print 'Cannot backfill realtime collectors'
        sys.exit(1)

    data = Fetcher(credentials, query).fetch()

    parsed_data = Parser().parse(data)

    Pusher(data_set_config, options).push(parsed_data)
