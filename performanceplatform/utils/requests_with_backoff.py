import logging
import time
from performanceplatform.collector.logging_setup import (
    extra_fields_from_exception)

# import everything from requests so that other code can import this package
# instead of requests
from requests import *

_MAX_RETRIES = 5


def get(url, *args, **kwargs):
    return __request_with_backoff('GET', url, *args, **kwargs)


def post(url, *args, **kwargs):
    return __request_with_backoff('POST', url, *args, **kwargs)


def __request_with_backoff(method, url, *args, **kwargs):
    delay = 10

    for n in range(_MAX_RETRIES):
        response = request(method, url, *args, **kwargs)
        code = response.status_code
        if code in [403, 502, 503]:
            retry_info = ('{} request for {} failed with code {} '
                          '(Attempt {} of {}).'.format(method, url, code,
                                                       n + 1, _MAX_RETRIES))
            if n + 1 < _MAX_RETRIES:
                retry_info += ' Retrying in {} seconds...'.format(delay)
            logging.info(retry_info)
            time.sleep(delay)
            delay *= 2
        else:
            response.raise_for_status()
            return response

    # we made _MAX_RETRIES requests but none worked
    logging.error('Max retries exceeded for {}'.format(url))
    try:
        response.raise_for_status()
    except Exception as e:
        logging.error('Requests Error: {}'.format(
            e.message,
            extra=extra_fields_from_exception(e)
        ))
        raise
