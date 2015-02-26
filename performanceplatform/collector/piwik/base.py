from performanceplatform.utils import requests_with_backoff


class BaseFetcher(object):
    def __init__(self, credentials, query):
        self.token_auth = credentials['token_auth']
        self.url = credentials['url']
        self.api_method = query['api_method']
        self.site_id = query['site_id']
        self.query_format = 'json'

    def _request_params(self):
        return {
            'method': self.api_method,
            'idSite': self.site_id,
            'format': self.query_format,
            'token_auth': self.token_auth
        }

    def _make_request(self):
        return requests_with_backoff.get(
            url="{url}?module=API".format(url=self.url),
            params=self._request_params()
        )

    def fetch(self):
        response = self._make_request()
        response.raise_for_status()
        return response.json()
