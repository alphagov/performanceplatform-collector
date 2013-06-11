import requests


class Backdrop(object):
    def __init__(self, url, token):
        self.url = url
        self.token = token

    def post(self, contents):
        headers = {
            "Authorization": "Bearer %s" % self.token,
            "Content-type": "application/json"
        }
        requests.post(
            url=self.url,
            headers=headers,
            data=contents
        )
