from pkgutil import extend_path
__path__ = extend_path(__path__, __name__)

from dshelpers import download_url

from performanceplatform.collector.gcloud.core import (
    nuke_local_database, save_raw_data, aggregate_and_save,
    push_aggregates)

from performanceplatform.collector.gcloud.sales_parser import (
    get_latest_csv_url)

from performanceplatform.client import DataSet


INDEX_URL = ('https://digitalmarketplace.blog.gov.uk'
             '/sales-accreditation-information/')


def main(credentials, data_set_config, query, options, start_at, end_at,
         filename=None):

    nuke_local_database()

    if filename is not None:
        with open(filename, 'r') as f:
            save_raw_data(f)
    else:
        save_raw_data(download_url(get_latest_csv_url(INDEX_URL)))

    aggregate_and_save()

    data_set = DataSet.from_config(data_set_config)
    data_set.empty_data_set()
    push_aggregates(data_set)
