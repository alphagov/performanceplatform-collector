# encoding: utf-8

from mock import Mock, patch, ANY
from performanceplatform.collector.ga.lib.helper import create_client


@patch("performanceplatform.collector.ga.lib.helper.from_credentials_db")
def test_create_client_handles_oauth2_credentials(mock_from_credentials_db):
    credentials = {
        "CLIENT_SECRETS": {
            "installed": {
                "auth_uri": "accounts.foo.com/o/oauth2/auth",
                "client_secret": "a client secret",
                "token_uri": "accounts.foo.com/o/oauth2/token",
                "client_email": "",
                "redirect_uris": ["urn:ietf:wg:oauth:2.0:oob", "oob"],
                "client_x509_cert_url": "",
                "client_id": "a client id",
                "auth_provider_x509_cert_url": "foo.com/oauth2/certs"
            },
        },
        "OAUTH2_CREDENTIALS": Mock()
    }
    client_secrets = credentials.get("CLIENT_SECRETS")["installed"]
    mock_storage_object = credentials.get("OAUTH2_CREDENTIALS")
    create_client(credentials)
    mock_from_credentials_db.assert_called_with(
        client_secrets, mock_storage_object, http_client=ANY, ga_hook=ANY)
