from httplib2 import *
from httplib2 import DEFAULT_MAX_REDIRECTS
import logging
import time
from performanceplatform.collector.logging_setup import (
    extra_fields_from_exception)

_MAX_RETRIES = 5


class HttpWithBackoff(Http):

    def request(self,
                uri,
                method="GET",
                body=None,
                headers=None,
                redirections=DEFAULT_MAX_REDIRECTS,
                connection_type=None):

        delay = 10

        for n in range(_MAX_RETRIES):
            response, content = super(HttpWithBackoff, self).request(
                uri,
                method,
                body,
                headers,
                redirections,
                connection_type)
            code = response.status

            # This is a shotgun approach to handle the error:
            #
            # - 503 Server Unavailable is sensible to use exponential backoff
            #   and retry.
            # - 502 Bad Gateway could be due to a backend system timing out
            #   (due to load) so again, we think it reasonable to retry.
            # - 403 Forbidden is a weird one. Google Analytics returns a 403
            #   status code but it can mean a bunch of things. See
            #   https://developers.google.com/analytics/devguides/reporting/core/v3/coreErrors
            #   So (for GA at least) we need to check one or more of:
            #   * the reason
            #   * the response body, test that it's JSON and parse it
            #
            # We probably want to make this backoff strategy pluggable, so
            # that we can handle:
            # * Google says not to retry on 503s, but we want to do that for
            #   backdrop
            # * how Piwik works
            # * how Pingdom works
            if code in [403, 502, 503]:
                retry_info = (
                    '{} request for {} failed with code {},'
                    'reason {} (Attempt {} of {}).'.format(method,
                                                           uri,
                                                           code,
                                                           response.reason,
                                                           n + 1,
                                                           _MAX_RETRIES))
                if n + 1 < _MAX_RETRIES:
                    retry_info += ' Retrying in {} seconds...'.format(delay)
                logging.info(retry_info)
                time.sleep(delay)
                delay *= 2
            else:
                # bad status codes should be handled by the Google API
                # client in the normal way
                return response, content

        # we made _MAX_RETRIES requests but none worked
        logging.error(
            'Max retries exceeded for {}'.format(uri),
            # HttpLibException doesnt exist but this fits with the format
            # for logging other request failures
            extra={
                'exception_class': 'HttpLibException',
                'exception_message': '{}: {}'.format(response.status,
                                                     response.reason)
            }
        )
        return response, content
