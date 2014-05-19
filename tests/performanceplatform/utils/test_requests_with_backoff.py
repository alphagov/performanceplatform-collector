from mock import patch, call
from nose.tools import assert_equal, assert_raises
import requests
from performanceplatform.utils import requests_with_backoff


def _response_generator(bad_status_code):
    bad_response = requests.Response()
    bad_response.status_code = bad_status_code

    good_response = requests.Response()
    good_response.status_code = 200
    good_response._content = str('Hello')

    yield bad_response
    yield bad_response
    yield good_response


@patch('time.sleep')
@patch('requests.request')
def _request_and_assert(request_call,
                        expected_call,
                        status_code,
                        mock_request,
                        mock_sleep):
    mock_request.side_effect = _response_generator(status_code)
    request_call()

    assert_equal(
        [call(10), call(20)],
        mock_sleep.call_args_list)

    assert_equal(
        [expected_call,
         expected_call,
         expected_call],
        mock_request.call_args_list)


class TestRequestsWithBackoff(object):

    # tests for GET
    @patch('requests.request')
    def test_get_proxies_requests_get(self, mock_request):
        requests_with_backoff.get('http://fake.com',
                                  kwarg1='a kwarg',
                                  kwarg2='another kwarg')

        mock_request.assert_called_with('GET',
                                        'http://fake.com',
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')

    def test_get_sleeps_correctly_on_503(self):
        _request_and_assert(
            lambda: requests_with_backoff.get('http://fake.com',
                                              kwarg1='a kwarg',
                                              kwarg2='another kwarg'),
            call('GET',
                 'http://fake.com',
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            503)

    def test_get_sleeps_correctly_on_502(self):
        _request_and_assert(
            lambda: requests_with_backoff.get('http://fake.com',
                                              kwarg1='a kwarg',
                                              kwarg2='another kwarg'),
            call('GET',
                 'http://fake.com',
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            502)

    def test_get_sleeps_correctly_on_403(self):
        _request_and_assert(
            lambda: requests_with_backoff.get('http://fake.com',
                                              kwarg1='a kwarg',
                                              kwarg2='another kwarg'),
            call('GET',
                 'http://fake.com',
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            403)

    @patch('requests.request')
    @patch('time.sleep')
    def test_get_does_not_sleep_on_404(self,
                                       mock_sleep,
                                       mock_request):

        not_found_response = requests.Response()
        not_found_response.status_code = 404
        mock_request.return_value = not_found_response

        with assert_raises(requests.HTTPError) as e:
            requests_with_backoff.get('http://fake.com',
                                      kwarg1='a kwarg',
                                      kwarg2='another kwarg')
        assert_equal(e.exception.response.status_code, 404)

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('GET',
                                        'http://fake.com',
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')

    @patch('requests.request')
    @patch('time.sleep')
    def test_get_raises_error_after_5_retries(self,
                                              mock_sleep,
                                              mock_request):

        service_unavailable_response = requests.Response()
        service_unavailable_response.status_code = 503
        mock_request.return_value = service_unavailable_response

        with assert_raises(requests.HTTPError) as e:
            requests_with_backoff.get('http://fake.com',
                                      kwarg1='a kwarg',
                                      kwarg2='another kwarg')
        assert_equal(e.exception.response.status_code, 503)

        assert_equal(
            [call(10), call(20), call(40), call(80), call(160)],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('GET',
                                        'http://fake.com',
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')

    # tests for POST
    @patch('requests.request')
    def test_post_proxies_requests_post(self, mock_request):
        requests_with_backoff.post('http://fake.com',
                                   data={},
                                   kwarg1='a kwarg',
                                   kwarg2='another kwarg')

        mock_request.assert_called_with('POST',
                                        'http://fake.com',
                                        data={},
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')

    def test_post_sleeps_correctly_on_503(self):
        _request_and_assert(
            lambda: requests_with_backoff.post('http://fake.com',
                                               data={},
                                               kwarg1='a kwarg',
                                               kwarg2='another kwarg'),
            call('POST',
                 'http://fake.com',
                 data={},
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            503)

    def test_post_sleeps_correctly_on_502(self):
        _request_and_assert(
            lambda: requests_with_backoff.post('http://fake.com',
                                               data={},
                                               kwarg1='a kwarg',
                                               kwarg2='another kwarg'),
            call('POST',
                 'http://fake.com',
                 data={},
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            502)

    def test_post_sleeps_correctly_on_403(self):
        _request_and_assert(
            lambda: requests_with_backoff.post('http://fake.com',
                                               data={},
                                               kwarg1='a kwarg',
                                               kwarg2='another kwarg'),
            call('POST',
                 'http://fake.com',
                 data={},
                 kwarg1='a kwarg',
                 kwarg2='another kwarg'),
            403)

    @patch('requests.request')
    @patch('time.sleep')
    def test_post_does_not_sleep_on_404(self,
                                        mock_sleep,
                                        mock_request):

        not_found_response = requests.Response()
        not_found_response.status_code = 404
        mock_request.return_value = not_found_response

        with assert_raises(requests.HTTPError) as e:
            requests_with_backoff.post('http://fake.com',
                                       data={},
                                       kwarg1='a kwarg',
                                       kwarg2='another kwarg')
        assert_equal(e.exception.response.status_code, 404)

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('POST',
                                        'http://fake.com',
                                        data={},
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')

    @patch('requests.request')
    @patch('time.sleep')
    def test_post_raises_error_after_5_retries(self,
                                               mock_sleep,
                                               mock_request):

        service_unavailable_response = requests.Response()
        service_unavailable_response.status_code = 503
        mock_request.return_value = service_unavailable_response

        with assert_raises(requests.HTTPError) as e:
            requests_with_backoff.post('http://fake.com',
                                       kwarg1='a kwarg',
                                       kwarg2='another kwarg')
        assert_equal(e.exception.response.status_code, 503)

        assert_equal(
            [call(10), call(20), call(40), call(80), call(160)],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('POST',
                                        'http://fake.com',
                                        kwarg1='a kwarg',
                                        kwarg2='another kwarg')
