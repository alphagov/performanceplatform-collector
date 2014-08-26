from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import run
from datetime import datetime
import pytz
import sys

from performanceplatform.client import DataSet
from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from performanceplatform.utils import statsd


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
        self._authenticate(credentials["CLIENT_SECRETS"],
                           credentials["STORAGE_PATH"])

    def _authenticate(self, client_secrets, storage_path):
        try:
            flow = flow_from_clientsecrets(client_secrets,
                                           scope=GOOGLE_API_SCOPE)
            storage = Storage(storage_path)
            credentials = storage.get()
            if credentials is None or credentials.invalid:
                credentials = run(flow, storage)

            self.service = build(
                serviceName="analytics",
                version="v3",
                http=credentials.authorize(HttpWithBackoff())
            )
        except HttpError as e:
            statsd.incr('ga.realtime.errors.{}.count'.format(e.resp.status))
            raise

    def execute_ga_query(self, query):
        return self.service.data().realtime().get(
            **query
        ).execute()

    def query(self, query):
        try:
            response = self.execute_ga_query(query)

            if "rows" in response:
                visitor_count = int(response["rows"][0][0])
            else:
                visitor_count = 0

            return visitor_count
        except HttpError as e:
            statsd.incr('ga.realtime.errors.{}.count'.format(e.resp.status))
            raise


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
