import datetime
import logging
import pytz
from performanceplatform.utils import requests_with_backoff
import json


class JsonEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            if obj.tzinfo is None:
                obj = obj.replace(tzinfo=pytz.UTC)
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class DataSet(object):
    """Client for writing to a Performance Platform data-set"""

    @staticmethod
    def from_config(config):
        return DataSet(
            config['url'],
            config['token'],
            config['dry_run']
        )

    def __init__(self, url, token, dry_run=False):
        self.url = url
        self.token = token
        self.dry_run = dry_run

    @staticmethod
    def _make_headers(token):
        return {
            "Authorization": "Bearer {}".format(token),
            "Content-type": "application/json"
        }

    @staticmethod
    def _encode_json(data):
        return json.dumps(data, cls=JsonEncoder)

    @staticmethod
    def _log_request(method, url, headers, body):
        logging.info("HTTP {} to '{}'\nheaders: {}\nbody: {}".format(
            method, url, headers, body))

    def post(self, records):
        headers = DataSet._make_headers(self.token)
        json_body = DataSet._encode_json(records)

        if self.dry_run:
            DataSet._log_request('POST', self.url, headers, json_body)
        else:
            response = requests_with_backoff.post(
                url=self.url,
                headers=headers,
                data=json_body
            )

            try:
                response.raise_for_status()
            except:
                logging.error('[PP: {}]\n{}'.format(
                    self.url,
                    response.text))
                raise

            logging.debug("[PP] " + response.text)
