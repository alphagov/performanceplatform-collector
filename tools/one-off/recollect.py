"""
Recollect configs that have changed dataType after the big collector refactor
"""
import requests
import argparse
import sys
import json
import os
from datetime import date, timedelta
import subprocess
from os.path import join, dirname

# These ones do not have data sets in stagecraft
# driving-test-practical-public/device-usage
# driving-test-practical-public/journey
# driving-test-practical-public/journey-help
# student-finance/browser-usage
# student-finance/device-usage
# student-finance/journey
# student-finance/site-traffic

CONFIGS = filter(None, """
carers-allowance/journey
govuk/browsers
govuk/devices
govuk/visitors
insidegov/visitors
lasting-power-of-attorney/journey
pay-foreign-marriage-certificates/journey
pay-legalisation-drop-off/journey
pay-legalisation-post/journey
pay-register-birth-abroad/journey
pay-register-death-abroad/journey
tier-2-visit-visa/devices
tier-2-visit-visa/journey
""".split('\n'))


def load_json(path):
    with open(path) as f:
        return json.load(f)


def get_token_path(config_path, token_id):
    return join(config_path, 'tokens', '{}.json'.format(token_id))


def get_token(config_path, token_id):
    token_path = get_token_path(config_path, token_id)
    return load_json(token_path)['token']


def get_query_path(config_path, config_id):
    return join(config_path, 'queries', '{}.json'.format(config_id))


def get_config(config_path, config_id):
    return load_json(get_query_path(config_path, config_id))


def get_base_url(config_path):
    return load_json(join(
        config_path, 'performanceplatform.json'))['backdrop_url']


def empty_dataset(config_path, data_set, token):
    url = '{}/{}/{}'.format(
        get_base_url(config_path),
        data_set['data-group'], data_set['data-type'])
    headers = {
        'Authorization': 'Bearer {}'.format(token),
        'Content-type': 'application/json',
    }
    print('EMPTY: {}'.format(url))
    response = requests.put(
        url=url, headers=headers, data='[]', verify=False)
    response.raise_for_status()


def months_ago(n):
    return date.today() - timedelta(days=31 * n)


def run_initial_backfill(config_path, config_id, config):
    print("BACKFILL ONE: {}".format(config_id))
    start_at = months_ago(2)
    end_at = None
    run_backfill(config_path, config_id, config, start_at, end_at)


def run_further_backfill(config_path, config_id, config):
    print("BACKFILL TWO: {}".format(config_id))
    start_at = months_ago(6)
    end_at = months_ago(2)
    run_backfill(config_path, config_id, config, start_at, end_at)


def run_backfill(config_path, config_id, config, start_at, end_at):
    collector_path = join(dirname(sys.executable), 'pp-collector')
    query_path = get_query_path(config_path, config_id)
    credentials_path = join(config_path, 'credentials', 'ga.json')
    token_path = get_token_path(config_path, config['token'])
    platform_path = join(config_path, 'performanceplatform.json')

    command = [collector_path,
               '-q', query_path,
               '-c', credentials_path,
               '-t', token_path,
               '-b', platform_path,
               '-s', start_at.isoformat()]

    if end_at is not None:
        command += ['-e', end_at.isoformat()]

    status = subprocess.call(command,
                             stderr=sys.stdout.fileno(),
                             stdout=sys.stdout.fileno())

    if status != 0:
        print("Failed!")
        raise SystemExit(1)


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('config_path',
                        help='New config path')

    args = parser.parse_args()

    for config_id in CONFIGS:
        config = get_config(args.config_path, config_id)
        token = get_token(args.config_path, config['token'])

        empty_dataset(args.config_path, config['data-set'], token)
        run_initial_backfill(args.config_path, config_id, config)

    for config_id in CONFIGS:
        config = get_config(args.config_path, config_id)
        run_further_backfill(args.config_path, config_id, config)


if __name__ == '__main__':
    main(sys.argv)
