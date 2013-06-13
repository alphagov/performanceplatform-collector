import datetime
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


class Bucket(object):
    """Client for writing to a backdrop bucket"""

    def __init__(self, url, token):
        self.url = url
        self.token = token

    def post(self, records):
        headers = {
            "Authorization": "Bearer %s" % self.token,
            "Content-type": "application/json"
        }
        response = requests.post(
            url=self.url,
            headers=headers,
            data=json.dumps(records, cls=JsonEncoder)
        )

        response.raise_for_status()
