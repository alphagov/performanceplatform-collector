import requests
import json


class Bucket(object):
    """Client for writing to a backdrop bucket

    >>> bucket = Bucket.from_target(\
        {"url":"http://somewhere/bucket", "token":"mytoken"})
    >>> bucket.post({"foo":"bar"})
    """

    @classmethod
    def from_target(cls, target):
        """Return Bucket from a target dict

        A target dict consists of a 'url' and a 'token', providing all
        the information required to query a bucket.
        """
        return cls(target['url'], target['token'])

    def __init__(self, url, token):
        self.url = url
        self.token = token

    def post(self, records):
        headers = {
            "Authorization": "Bearer %s" % self.token,
            "Content-type": "application/json"
        }
        requests.post(
            url=self.url,
            headers=headers,
            data=json.dumps(records)
        )
