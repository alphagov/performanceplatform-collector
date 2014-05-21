import logging
import time

# import everything from requests so that other code can import this package
# instead of requests
from requests import *

_MAX_RETRIES = 5


def get(url, *args, **kwargs):
    __request_with_backoff('GET', url, *args, **kwargs)


def post(url, *args, **kwargs):
    __request_with_backoff('POST', url, *args, **kwargs)


def __request_with_backoff(method, url, *args, **kwargs):
    delay = 10

    for n in range(_MAX_RETRIES):
        response = request(method, url, *args, **kwargs)
        code = response.status_code
        if code in [403, 502, 503]:
            logging.info('{} request for {} failed with code {}. Retrying in '
                         '{} seconds...'.format(method, url, code, delay))
            time.sleep(delay)
            delay *= 2
        else:
            response.raise_for_status()
            return response

    # we made _MAX_RETRIES requests but none worked
    logging.error('Max retries exceeded for {}'.format(url))
    response.raise_for_status()
