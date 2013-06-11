import requests


class Backdrop(object):
    def __init__(self, location):
        self.location = location

    def post(self, contents, bucket, token):
        headers = {
            "Authorization": token,
            "Content-type": "application/json"
        }
        requests.post(
            url=self.location + "/%s" % bucket,
            headers=headers,
            data=contents
        )
