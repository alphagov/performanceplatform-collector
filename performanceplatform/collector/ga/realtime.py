from datetime import datetime
import pytz
import sys

from performanceplatform.collector.ga.lib.helper import create_client

from performanceplatform.client import DataSet


GOOGLE_API_SCOPE = "https://www.googleapis.com/auth/analytics"


class Collector(object):
    def __init__(self, credentials):
        self._realtime = Realtime(credentials)

    def send_records_for(self, query, to):
        data_set = DataSet.from_config(to)

        visitor_count = self._realtime.query(query)

        record = self._create_record(visitor_count,
                                     query.get('filters', ''))

        data_set.post(record)

    def _create_record(self, visitor_count, for_url):
        timestamp = _timestamp()
        return {
            "_timestamp": timestamp,
            "_id": timestamp,
            "unique_visitors": visitor_count,
            "for_url": for_url
        }


class Realtime(object):
    def __init__(self, credentials):
        self._client = create_client(credentials)

    def execute_ga_query(self, query):
        return self._client._service.data().realtime().get(
            **query
        ).execute()

    def query(self, query):
        response = self.execute_ga_query(query)

        if "rows" in response:
            visitor_count = int(response["rows"][0][0])
        else:
            visitor_count = 0

        return visitor_count


def _timestamp():
    timezone = pytz.UTC
    timestamp = datetime.now(timezone).replace(microsecond=0)
    return timestamp.isoformat()


def main(credentials, data_set, query, options, start_at, end_at):
    if start_at or end_at:
        print 'Cannot backfill realtime collectors'
        sys.exit(1)
    collector = Collector(credentials)
    collector.send_records_for(query, data_set)
