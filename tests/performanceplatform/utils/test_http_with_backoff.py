from mock import patch, call
from nose.tools import assert_equal
from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from performanceplatform.utils.http_with_backoff import parse_reason
from httplib2 import Response


@patch('time.sleep')
@patch('performanceplatform.utils.http_with_backoff'
       '.Http.request')
def _request_and_assert(request_call,
                        expected_call,
                        status_code,
                        mock_request,
                        mock_sleep):

    """
    Send a passed in request to HttpWithBackoff and check
    that the wrapped requests package makes an equivalent request
    with identical parameters. Also check that sleep was called
    in the way expected when the first two requests are bad and
    have the passed in status_code.
    """
    def _response_generator(bad_status_code):
        # TODO: Should this be a mock? We shouldn't know any more than
        # necessary about the interface of Response
        bad_response = Response({})
        bad_response.status = bad_status_code

        good_response = Response({})
        good_response.status = 200
        content = 'some content'

        return [(bad_response, content),
                (bad_response, content),
                (good_response, content)]

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

    def test_request_sleeps_correctly_on_503(self):
        _request_and_assert(
            lambda: HttpWithBackoff().request('http://fake.com',
                                              method="PUT",
                                              body="bar",
                                              headers="foo",
                                              redirections=10,
                                              connection_type="wibble"),
            call('http://fake.com', "PUT", "bar", "foo", 10, "wibble"), 503)

    def test_request_sleeps_correctly_on_502(self):
        _request_and_assert(
            lambda: HttpWithBackoff().request('http://fake.com',
                                              method="PUT",
                                              body="bar",
                                              headers="foo",
                                              redirections=10,
                                              connection_type="wibble"),
            call('http://fake.com', "PUT", "bar", "foo", 10, "wibble"), 502)

    def test_request_sleeps_correctly_on_403(self):
        _request_and_assert(
            lambda: HttpWithBackoff().request('http://fake.com',
                                              method="PUT",
                                              body="bar",
                                              headers="foo",
                                              redirections=10,
                                              connection_type="wibble"),
            call('http://fake.com', "PUT", "bar", "foo", 10, "wibble"), 403)

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
    def test_request_raises_error_after_5_retries(self,
                                                  mock_sleep,
                                                  mock_request):

        service_unavailable_response = Response({})
        service_unavailable_response.status = 503
        content = 'content'
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
