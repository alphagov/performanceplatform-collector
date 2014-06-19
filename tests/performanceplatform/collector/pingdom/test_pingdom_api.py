from datetime import date, datetime
from time import mktime
import unittest
from hamcrest import assert_that, is_, instance_of
from mock import patch, Mock
import pytz
import requests
from performanceplatform.collector.pingdom.core import Pingdom


def unix_timestamp(_datetime):
    return mktime(_datetime.timetuple())


class TestPingdomApi(unittest.TestCase):
    def setUp(self):
        self.config = {
            "user": "foo@bar.com",
            "password": "secret",
            "app_key": "12345"
        }

    def test_init_from_config(self):
        pingdom = Pingdom(self.config)
        assert_that(pingdom, is_(instance_of(Pingdom)))

    def test_querying_for_stats(self):

        def mock_send_request(*args, **kwargs):
            if kwargs["path"] == "summary.performance/12345":
                return {"summary": {"hours": []}}
            if kwargs["path"] == "checks":
                return {"checks": [{"name": "Foo", "id": 12345}]}

        pingdom = Pingdom(self.config)
        pingdom._make_request = Mock(name='_make_request')
        pingdom._make_request.side_effect = mock_send_request

        uptime = pingdom.stats(
            check_name='Foo',
            start=datetime(2012, 12, 31, 18, 0, 0),
            end=datetime(2013, 1, 1, 18, 0, 0)
        )

        pingdom._make_request.assert_called_with(
            path="summary.performance/12345",
            url_params={
                "includeuptime": "true",
                "from": unix_timestamp(datetime(2012, 12, 31, 18, 0, 0)),
                "to": unix_timestamp(datetime(2013, 1, 1, 18, 0, 0)),
                "resolution": "hour"
            }
        )

        assert_that(uptime, is_([]))

    @patch("performanceplatform.utils.requests_with_backoff.get")
    def test_response_unixtime_converted_to_isodate(self, mock_get_request):
        mock_response = Mock()
        mock_response.json.return_value = {
            'summary': {
                'hours': [{'starttime': 1356998400}, {'starttime': 1356998500}]
            }
        }
        mock_get_request.return_value = mock_response

        pingdom = Pingdom(self.config)

        mock_check_id = Mock()
        mock_check_id.return_value = '12345'
        pingdom.check_id = mock_check_id
        uptime = pingdom.stats(
            check_name='Foo',
            start=date(2012, 12, 31),
            end=date(2013, 1, 1))

        assert_that(uptime[0]['starttime'],
                    is_(datetime(2013, 1, 1, 0, tzinfo=pytz.UTC)))
        assert_that(uptime[1]['starttime'],
                    is_(datetime(2013, 1, 1, 0, 1, 40, tzinfo=pytz.UTC)))

    def test_stats_returns_none_when_there_is_an_error(self):
        pingdom = Pingdom(self.config)
        pingdom._make_request = Mock(name='_make_request')
        pingdom._make_request.side_effect = requests.exceptions.HTTPError()

        mock_check_id = Mock()
        mock_check_id.return_value = '12345'
        pingdom.check_id = mock_check_id
        uptime = pingdom.stats(
            check_name="don't care",
            start=date(2012, 12, 31),
            end=date(2013, 1, 1))
        assert_that(uptime, is_(None))
