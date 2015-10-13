from performanceplatform.utils.http_with_backoff import HttpWithBackoff
from gapy.client import (
    from_private_key,
    from_secrets_file,
    from_credentials_db,
)
from performanceplatform.utils import statsd


def track_ga_api_usage(kwargs):
    statsd.incr('ga.core.{}.count'.format(kwargs['ids'].replace(':', '')))


def create_client(credentials):
    if "CLIENT_SECRETS" in credentials and "STORAGE_PATH" in credentials:
        return from_secrets_file(
            credentials['CLIENT_SECRETS'],
            storage_path=credentials['STORAGE_PATH'],
            http_client=HttpWithBackoff(),
            ga_hook=track_ga_api_usage,
        )
    elif "ACCOUNT_NAME" in credentials:
        return from_private_key(
            credentials['ACCOUNT_NAME'],
            private_key_path=credentials['PRIVATE_KEY'],
            storage_path=credentials['STORAGE_PATH'],
            http_client=HttpWithBackoff(),
            ga_hook=track_ga_api_usage,
        )
    else:
        return from_credentials_db(
            credentials['CLIENT_SECRETS']['installed'],
            credentials['OAUTH2_CREDENTIALS'],
            http_client=HttpWithBackoff(),
            ga_hook=track_ga_api_usage,
        )
