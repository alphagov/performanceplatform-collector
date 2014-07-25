"""
Generate required credentials files from a downloaded client_secret.json.

When setting up a Google API account you are provided with a
client_secret.json. You need to generate refresh and access tokens to use the
API.
For more information see:

http://bit.ly/py-oauth-docs

Call this tool with the path to your downloaded client_secret.json as
the only argument. The credentials file in ./config/credentials.json will be
updated.
"""
import argparse
import json
from os.path import abspath, exists as path_exists
from os import makedirs

from gapy.client import from_secrets_file
import oauth2client.tools


def copy_json(input_path, output_path):
    with open(input_path) as input:
        with open(output_path, "w+") as output:
            json.dump(
                json.load(input),
                output,
                indent=2)


def create_secret_file(client_id, client_secret):
    secret_path = '/tmp/client_secret'
    with open(secret_path, 'w+') as f:
        data = {
            "installed": {
                "auth_provider_x509_cert_url":
                    "https://www.googleapis.com/oauth2/v1/certs",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "client_email": "",
                "client_id": client_id,
                "client_secret": client_secret,
                "client_x509_cert_url": "",
                "redirect_uris": [
                    "urn:ietf:wg:oauth:2.0:oob",
                    "oob"
                ],
                "token_uri": "https://accounts.google.com/o/oauth2/token"
            }
        }
        json.dump(data, f)

    return secret_path


def generate_google_credentials(client_id, client_secret):
    # Prevent oauth2client from trying to open a browser
    # This is run from inside the VM so there is no browser
    oauth2client.tools.FLAGS.auth_local_webserver = False

    print("When prompted you must visit the verification URL while logged " +
          "in as the user that owns the client ID""")

    if not path_exists(abspath("./creds/ga/")):
        makedirs("./creds/ga")
    storage_path = abspath("./creds/ga/storage.db")
    secret_path = create_secret_file(client_id, client_secret)
    from_secrets_file(secret_path, storage_path=storage_path)

    with open(storage_path) as f:
        storage_data = json.load(f)
        print("Refresh token: {}".format(storage_data['refresh_token']))
    with open('./creds/ga.json', 'w+') as f:
        credentials = {
            "CLIENT_SECRETS": secret_path,
            "STORAGE_PATH": storage_path,
        }
        json.dump(credentials, f, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)

    parser.add_argument(
        'client_id',
        help='client ID')
    parser.add_argument(
        'client_secret',
        help='client secret')

    args = parser.parse_args()

    generate_google_credentials(args.client_id, args.client_secret)
