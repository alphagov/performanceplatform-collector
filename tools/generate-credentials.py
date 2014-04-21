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
from os.path import abspath

from gapy.client import from_secrets_file
import oauth2client.tools


def copy_json(input_path, output_path):
    with open(input_path) as input:
        with open(output_path, "w+") as output:
            json.dump(
                json.load(input),
                output,
                indent=2)


def generate_stuff(client_secret):
    # Prevent oauth2client from trying to open a browser
    # This is run from inside the VM so there is no browser
    oauth2client.tools.FLAGS.auth_local_webserver = False

    storage_path = abspath("./config/storage.db")
    secret_path = abspath("./config/client_secret.json")
    from_secrets_file(
        client_secret,
        storage_path=storage_path)

    copy_json(client_secret, secret_path)

    with open('./config/credentials.json', 'w+') as f:
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
        'client_secret',
        help='path to the client secrets file from the Google API Console')

    args = parser.parse_args()

    generate_stuff(args.client_secret)
