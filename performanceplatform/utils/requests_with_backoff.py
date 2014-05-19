import logging
import requests
import time

_MAX_RETRIES = 5

# for code that wants to import exceptions from this file
exceptions = requests.exceptions


def get(url, *args, **kwargs):
    request_with_backoff('GET', url, **kwargs)


def post(url, *args, **kwargs):
    request_with_backoff('POST', url, **kwargs)


def request_with_backoff(method, url, **kwargs):
    delay = 10

    for n in range(_MAX_RETRIES):
        response = requests.request(method, url, **kwargs)
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
