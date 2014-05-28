from datetime import datetime
import pytz
from performanceplatform.utils import requests_with_backoff
import time
import logging


def _send_authenticated_pingdom_request(path, user, password, app_key,
                                        url_params):
    response = requests_with_backoff.get(
        url="https://api.pingdom.com/api/2.0/" + path,
        auth=(user, password),
        headers={
            'App-key': app_key
        },
        params=url_params
    )

    response.raise_for_status()

    return response.json()


class Pingdom(object):
    def __init__(self, config):
        self.user = config['user']
        self.password = config['password']
        self.app_key = config['app_key']
        self.API_LOCATION = "https://api.pingdom.com/api/2.0/"

    def _make_request(self, path, url_params={}):
        response = requests_with_backoff.get(
            url=self.API_LOCATION + path,
            auth=(self.user, self.password),
            headers={
                "App-Key": self.app_key
            },
            params=url_params
        )
        return response

    def _build_response(self, response):
        hours = response['summary']['hours']
        new_hours = []
        for hour in hours:
            hour.update({'starttime': datetime.fromtimestamp(
                hour['starttime'],
                tz=pytz.UTC
            )})
            new_hours.append(hour)
        return new_hours

    def stats(self, check_name, start, end):
        app_code = self.check_id(check_name)
        params = {
            "includeuptime": "true",
            "from": time.mktime(start.timetuple()),
            "to": time.mktime(end.timetuple()),
            "resolution": "hour"
        }
        path = "summary.performance/" + str(app_code)

        try:
            return self._build_response(_send_authenticated_pingdom_request(
                path=path,
                user=self.user,
                password=self.password,
                app_key=self.app_key,
                url_params=params
            ))
        except requests_with_backoff.exceptions.HTTPError as e:
            logging.error("Request to pingdom failed: %s" % str(e))

    def check_id(self, name):
        checks = _send_authenticated_pingdom_request(
            path="checks",
            user=self.user,
            password=self.password,
            app_key=self.app_key,
            url_params=None
        )

        check_to_find = [check for check in checks["checks"]
                         if check["name"] == name]

        return check_to_find[0]["id"]


class Collector(object):
    def __init__(self, config):
        pass
