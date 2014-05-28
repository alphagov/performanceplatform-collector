from mock import patch, call
from nose.tools import assert_equal, assert_raises, assert_is
import requests
from performanceplatform.utils import requests_with_backoff


def _make_good_response():
    good_response = requests.Response()
    good_response.status_code = 200
    good_response._content = str('Hello')
    return good_response


def _make_bad_response(bad_status_code):
    bad_response = requests.Response()
    bad_response.status_code = bad_status_code
    return bad_response


@patch('time.sleep')
@patch('performanceplatform.utils.requests_with_backoff.request')
def _request_and_assert(request_call,
                        expected_call,
                        status_code,
                        mock_request,
                        mock_sleep):

    """
    Send a passed in request to requests_with_backoff and check
    that the wrapped requests package makes an equivalent request
    with identical parameters. Also check that sleep was called
    in the way expected when the first two requests are bad and
    have the passed in status_code.
    """
    def _response_generator(bad_status_code):
        bad_response = _make_bad_response(bad_status_code)
        good_response = _make_good_response()

        yield bad_response
        yield bad_response
        yield good_response

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
    @patch('performanceplatform.utils.requests_with_backoff.request')
    def test_get_proxies_requests_get(self, mock_request):
        good_response = _make_good_response()
        mock_request.return_value = good_response

        response = requests_with_backoff.get('http://fake.com',
                                             kwarg1='a kwarg',
                                             kwarg2='another kwarg')
        assert_is(response, good_response)

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

    @patch('performanceplatform.utils.requests_with_backoff.request')
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

    @patch('performanceplatform.utils.requests_with_backoff.request')
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
    @patch('performanceplatform.utils.requests_with_backoff.request')
    def test_post_proxies_requests_post(self, mock_request):
        good_response = _make_good_response()
        mock_request.return_value = good_response

        response = requests_with_backoff.post('http://fake.com',
                                              data={},
                                              kwarg1='a kwarg',
                                              kwarg2='another kwarg')

        assert_is(response, good_response)
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

    @patch('performanceplatform.utils.requests_with_backoff.request')
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

    @patch('performanceplatform.utils.requests_with_backoff.request')
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
