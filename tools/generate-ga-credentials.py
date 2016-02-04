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

from oauth2client import tools

from gapy.client import from_secrets_file


def copy_json(input_path, output_path):
    with open(input_path) as input:
        with open(output_path, "w+") as output:
            json.dump(
                json.load(input),
                output,
                indent=2)


def generate_google_credentials(args):
    client_secret = args.client_secret
    if not path_exists(abspath("./creds/ga/")):
        makedirs("./creds/ga")
    storage_path = abspath("./creds/ga/storage.db")
    secret_path = abspath("./creds/ga/client_secret.json")
    from_secrets_file(
        client_secret,
        storage_path=storage_path,
        flags=args)

    copy_json(client_secret, secret_path)

    with open('./creds/ga.json', 'w+') as f:
        credentials = {
            "CLIENT_SECRETS": secret_path,
            "STORAGE_PATH": storage_path,
        }
        json.dump(credentials, f, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
        parents=[tools.argparser])

    parser.add_argument(
        'client_secret',
        help='path to the client secrets file from the Google API Console')

    args = parser.parse_args()

    # This script is run from within the VM so
    # disable need for a browser
    args.noauth_local_webserver = True

    generate_google_credentials(args)
