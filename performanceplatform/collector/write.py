import datetime
import logging
import pytz
import requests
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

    def post(self, records):
        headers = {
            "Authorization": "Bearer %s" % self.token,
            "Content-type": "application/json"
        }
        json_body = json.dumps(records, cls=JsonEncoder)

        if self.dry_run:
            logging.info(self.url)
            logging.info(headers)
            logging.info(json_body)
        else:
            response = requests.post(
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
