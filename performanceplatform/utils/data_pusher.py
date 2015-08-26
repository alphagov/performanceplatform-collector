from performanceplatform.client import DataSet
import logging


class Pusher(object):
    def __init__(self, target_data_set_config, options):
        self.data_set_client = DataSet.from_config(target_data_set_config)
        self.chunk_size = options.get('chunk-size', 100)
        self.options = options

    def push(self, data):
        if self.options.get('empty_data_set'):
            self.data_set_client.empty_data_set()
        if data:
            self.data_set_client.post(
                data, chunk_size=self.chunk_size)
        else:
            logging.info("Doing nothing - no data to push")
