from datetime import datetime
import pytz
from performanceplatform.utils import requests_with_backoff
from performanceplatform.collector.logging_setup import (
    extra_fields_from_exception )
import time
import logging


class Pingdom(object):
    def __init__(self, config):
        self.user = config['user']
        self.password = config['password']
        self.app_key = config['app_key']
        self.API_LOCATION = "https://api.pingdom.com/api/2.0/"

    def _make_request(self, path, url_params=None):
        if url_params is None:
            url_params = {}
        response = requests_with_backoff.get(
            url=self.API_LOCATION + path,
            auth=(self.user, self.password),
            headers={
                "App-Key": self.app_key
            },
            params=url_params
        )
        response.raise_for_status()

        return response.json()

    def _build_response(self, data):
        hours = data['summary']['hours']
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
            return self._build_response(self._make_request(
                path=path,
                url_params=params
            ))
        except requests_with_backoff.exceptions.HTTPError as e:
            logging.error("Request to pingdom failed: %s" % str(e),
                          extra=extra_fields_from_exception(e))

    def check_id(self, name):
        checks = self._make_request(path="checks")

        check_to_find = [check for check in checks["checks"]
                         if check["name"] == name]

        return check_to_find[0]["id"]
