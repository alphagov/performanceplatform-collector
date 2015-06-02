from httplib2 import *
from httplib2 import DEFAULT_MAX_REDIRECTS
import json
import logging
import time
from performanceplatform.collector.logging_setup import (
    extra_fields_from_exception)

_MAX_RETRIES = 5


def abort_message(method, uri, status, reason):
    return (
        '{} request for {} failed with code {}, '
        'reason {}. Aborting. See '
        'https://developers.google.com'
        '/analytics/devguides/reporting/core/v3/coreErrors'
        ' for more details'.format(
            method,
            uri,
            status,
            reason))


def notify_operator(method, uri, status, reason):
    logging.error(abort_message(method, uri, status, reason))


def parse_reason(response, content):
    """
    Google return a JSON document describing the error. We should parse this
    and extract the error reason. See
    https://developers.google.com/analytics/devguides/reporting/core/v3/coreErrors
    """
    def first_error(data):
        errors = data['error']['errors']
        if len(errors) > 1:
            # we have more than one error. We should capture that?
            logging.info('Received {} errors'.format(len(errors)))
        return errors[0]

    try:
        json_data = json.loads(content)
        return first_error(json_data)['reason']
    except:
        return response.reason


class ResponseAction:

    def __init__(self):
        self.should_retry = False
        self.retry_info = ''


def GABackoff(response, content, method, uri):
    response_action = ResponseAction()

    if response.status not in [400, 401, 403, 503]:
        return response_action

    reason = parse_reason(response, content)

    if response.status in [400, 401]:
        # --------------------------------------------------------------------
        # Do not retry without fixing the problem
        # notify someone with appropriate actionable data
        # --------------------------------------------------------------------
        notify_operator(method, uri, response.status, reason)
        return response_action

    if response.status == 503:
        # --------------------------------------------------------------------
        # Server returned an error. Do not retry this query more than once.
        # --------------------------------------------------------------------
        # Not sure we can action anything as a result of this? Just note it.
        logging.info(abort_message(method, uri, response.status, reason))
        return response_action

    if response.status == 403:
        if reason in [
            "insufficientPermissions",
            "dailyLimitExceeded",
            "usageLimits.userRateLimitExceededUnreg",
        ]:
            # ----------------------------------------------------------------
            # insufficientPermissions indicates that the user does not have
            # sufficient permissions for the entity specified in the query.
            #
            # Do not retry without fixing the problem. You need to get
            # sufficient permissions to perform the operation on the specified
            # entity.
            # ----------------------------------------------------------------
            # dailyLimitExceeded indicates that user has exceeded the daily
            # quota (either per project or per view (profile)).
            #
            # Do not retry without fixing the problem. You have used up your
            # daily quota. See
            # https://developers.google.com/analytics/devguides/reporting/core/v3/limits-quotas
            # ----------------------------------------------------------------
            # usageLimits.userRateLimitExceededUnreg indicates that the
            # application needs to be registered in the Google Developers
            # Console.
            #
            # Do not retry without fixing the problem. You need to register in
            # Developers Console to get the full API quota.
            # See https://console.developers.google.com/
            # ----------------------------------------------------------------
            notify_operator(method, uri, response.status, reason)
            return response_action
        elif reason in ["userRateLimitExceeded", "quotaExceeded"]:
            # ----------------------------------------------------------------
            # userRateLimitExceeded indicates that the user rate limit has
            # been exceeded. The maximum rate limit is 10 qps per IP address.
            # The default value set in Google Developers Console is 1 qps per
            # IP address. You can increase this limit in the Google Developers
            # Console to a maximum of 10 qps.
            #
            # Retry using exponential back-off. You need to slow down the rate
            # at which you are sending the requests
            # ----------------------------------------------------------------
            # quotaExceeded indicates that the 10 concurrent requests per view
            # (profile) in the Core Reporting API has been reached.
            #
            # Retry using exponential back-off. You need to wait for at least
            # one in-progress request for this view (profile) to complete.
            # ----------------------------------------------------------------

            # fall through to retry handling below
            response_action.should_retry = True
        else:
            # Unhandled 403 status
            notify_operator(method, uri, response.status, reason)
            return response_action

    # If we got this far, then we're going to retry the request. Capture more
    # detail about why.
    retry_info = ('{} request for {} failed with code {}, '
                  'reason {}'.format(method,
                                     uri,
                                     response.status,
                                     reason))
    response_action.retry_info = retry_info

    return response_action


class HttpWithBackoff(Http):

    def __init__(self, cache=None, timeout=None,
                 proxy_info=None,
                 ca_certs=None, disable_ssl_certificate_validation=False,
                 backoff_strategy_predicate=GABackoff):
        dscv = disable_ssl_certificate_validation
        super(HttpWithBackoff, self).__init__(
            cache=cache, timeout=timeout,
            proxy_info=proxy_info,
            ca_certs=ca_certs,
            disable_ssl_certificate_validation=dscv)
        if backoff_strategy_predicate:
            self._backoff_strategy_predicate = backoff_strategy_predicate
        else:
            self._backoff_strategy_predicate = GABackoff

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

            response_action = self._backoff_strategy_predicate(
                response, content, method, uri)

            if response_action.should_retry:
                retry_info = response_action.retry_info
                retry_info += '(Attempt {} of {})'.format(n + 1, _MAX_RETRIES)
                if n + 1 < _MAX_RETRIES:
                    retry_info += ' Retrying in {} seconds...'.format(delay)
                logging.info(retry_info)
                time.sleep(delay)
                delay *= 2
            else:
                return response, content

        # we made _MAX_RETRIES requests but none worked
        logging.error(
            'Max retries exceeded for {}'.format(uri),
            # HttpLibException doesn't exist but this fits with the format
            # for logging other request failures
            extra={
                'exception_class': 'HttpLibException',
                'exception_message': '{}: {}'.format(response.status,
                                                     response.reason)
            }
        )

        return response, content
