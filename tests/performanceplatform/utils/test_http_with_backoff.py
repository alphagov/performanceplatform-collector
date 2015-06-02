from mock import patch, call
from nose.tools import assert_equal
from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from performanceplatform.utils.http_with_backoff import parse_reason
from httplib2 import Response
import logging


@patch('time.sleep')
@patch('performanceplatform.utils.http_with_backoff'
       '.Http.request')
def _request_and_assert(request_call,
                        expected_call,
                        status_code,
                        reason,
                        mock_request,
                        mock_sleep):
    """
    Send a passed in request to HttpWithBackoff and check
    that the wrapped requests package makes an equivalent request
    with identical parameters. Also check that sleep was called
    in the way expected when the first two requests are bad and
    have the passed in status_code.
    """
    def _response_generator(bad_status_code, reason):
        # TODO: Should this be a mock? We shouldn't know any more than
        # necessary about the interface of Response
        bad_response = Response({})
        bad_response.status = bad_status_code
        bad_response.reason = reason

        good_response = Response({})
        good_response.status = 200
        content = 'some content'

        return [(bad_response, content),
                (bad_response, content),
                (good_response, content)]

    mock_request.side_effect = _response_generator(status_code, reason)
    request_call()

    assert_equal(
        [call(10), call(20)],
        mock_sleep.call_args_list)

    assert_equal(
        [expected_call,
         expected_call,
         expected_call],
        mock_request.call_args_list)


def google_error_response(code, reason):
    return """{{
 "error": {{
  "errors": [
   {{
    "domain": "global",
    "reason": "{reason}",
    "message": "foo",
    "locationType": "parameter",
    "location": "max-results"
   }}
  ],
  "code": {code},
  "message": "foo"
 }}
}}""".format(**{'code': code, 'reason': reason})


class TestHttpWithBackoff(object):

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    def test_request_with_only_uri_calls_Http_with_default_params(
            self,
            mock_request):

        response = Response({})
        response.status = 200
        mock_request.return_value = (response, 'content')
        HttpWithBackoff().request('http://fake.com')

        mock_request.assert_called_with('http://fake.com',
                                        "GET",
                                        None,
                                        None,
                                        5,
                                        None)

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    def test_request_with_full_args_calls_Http_with_args(
            self,
            mock_request):

        response = Response({})
        response.status = 200
        mock_request.return_value = (response, 'content')
        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    def test_request_does_not_sleep_on_503(self,
                                           mock_sleep,
                                           mock_request):
        backendError = Response({})
        backendError.status = 503
        content = google_error_response(503, "backendError")

        mock_request.return_value = (backendError,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    def test_request_does_not_sleep_on_502(self,
                                           mock_sleep,
                                           mock_request):
        backendError = Response({})
        backendError.status = 502
        content = google_error_response(502, "we made it up")

        mock_request.return_value = (backendError,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")

    def test_request_sleeps_correctly_on_403_userRateLimitExceeded(self):
        _request_and_assert(
            lambda: HttpWithBackoff().request('http://fake.com',
                                              method="PUT",
                                              body="bar",
                                              headers="foo",
                                              redirections=10,
                                              connection_type="wibble"),
            call('http://fake.com', "PUT", "bar", "foo", 10, "wibble"), 403, "userRateLimitExceeded")

    def test_request_sleeps_correctly_on_403_quotaExceeded(self):
        _request_and_assert(
            lambda: HttpWithBackoff().request('http://fake.com',
                                              method="PUT",
                                              body="bar",
                                              headers="foo",
                                              redirections=10,
                                              connection_type="wibble"),
            call('http://fake.com', "PUT", "bar", "foo", 10, "wibble"), 403, "quotaExceeded")

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    def test_request_does_not_sleep_on_404(self,
                                           mock_sleep,
                                           mock_request):

        not_found_response = Response({})
        not_found_response.status = 404
        content = 'content'
        mock_request.return_value = (not_found_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    @patch('logging.error')
    def test_request_treats_400_as_bad_error(self,
                                             mock_error,
                                             mock_sleep,
                                             mock_request):

        bad_response = Response({})
        bad_response.status = 400
        content = 'content'
        mock_request.return_value = (bad_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")
        mock_error.assert_called()

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    @patch('logging.error')
    def test_request_treats_401_as_bad_error(self,
                                             mock_error,
                                             mock_sleep,
                                             mock_request):

        bad_response = Response({})
        bad_response.status = 401
        content = 'content'
        mock_request.return_value = (bad_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")
        mock_error.assert_called()

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    @patch('logging.error')
    def test_request_treats_403_insufficientPermissions_as_bad_error(self,
                                                                     mock_error,
                                                                     mock_sleep,
                                                                     mock_request):

        bad_response = Response({})
        bad_response.status = 403
        content = google_error_response(403, 'insufficientPermissions')
        mock_request.return_value = (bad_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")
        mock_error.assert_called()

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    @patch('logging.error')
    def test_request_treats_403_dailyLimitExceeded_as_bad_error(self,
                                                                mock_error,
                                                                mock_sleep,
                                                                mock_request):

        bad_response = Response({})
        bad_response.status = 403
        content = google_error_response(403, 'dailyLimitExceeded')
        mock_request.return_value = (bad_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")
        mock_error.assert_called()

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    @patch('logging.error')
    def test_request_treats_403_usageLimits_userRateLimitExceededUnreg_as_bad_error(self,
                                                                                    mock_error,
                                                                                    mock_sleep,
                                                                                    mock_request):

        bad_response = Response({})
        bad_response.status = 403
        content = google_error_response(
            403, 'usageLimits.userRateLimitExceededUnreg')
        mock_request.return_value = (bad_response,
                                     content)

        HttpWithBackoff().request('http://fake.com',
                                  method="PUT",
                                  body="bar",
                                  headers="foo",
                                  redirections=10,
                                  connection_type="wibble")

        assert_equal(
            [],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")
        mock_error.assert_called()

    @patch('performanceplatform.utils.http_with_backoff.Http.request')
    @patch('time.sleep')
    def test_request_raises_error_after_5_retries(self,
                                                  mock_sleep,
                                                  mock_request):

        service_unavailable_response = Response({})
        service_unavailable_response.status = 403
        content = google_error_response(403, "userRateLimitExceeded")
        logging.info(content)

        mock_request.return_value = (service_unavailable_response,
                                     content)

        response = HttpWithBackoff().request('http://fake.com',
                                             method="PUT",
                                             body="bar",
                                             headers="foo",
                                             redirections=10,
                                             connection_type="wibble")

        assert_equal(response, (service_unavailable_response,
                                content))

        assert_equal(
            [call(10), call(20), call(40), call(80), call(160)],
            mock_sleep.call_args_list)

        mock_request.assert_called_with('http://fake.com',
                                        "PUT",
                                        "bar",
                                        "foo",
                                        10,
                                        "wibble")

    def test_parse_reason_with_empty_content(self):
        assert_equal(parse_reason(Response({}), ''), 'Ok')

    def test_parse_reason_with_html_content(self):
        assert_equal(parse_reason(Response({}), '<strong>lol</strong>'), 'Ok')

    def test_parse_reason_with_json_content(self):
        assert_equal(parse_reason(Response({}), """{
 "error": {
  "errors": [
   {
    "domain": "global",
    "reason": "invalidParameter",
    "message": "Invalid value '-1' for max-results. Value must be within the range: [1, 1000]",
    "locationType": "parameter",
    "location": "max-results"
   }
  ],
  "code": 400,
  "message": "Invalid value '-1' for max-results. Value must be within the range: [1, 1000]"
 }
}"""), 'invalidParameter')
