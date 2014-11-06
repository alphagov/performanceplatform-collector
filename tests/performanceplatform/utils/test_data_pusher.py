import unittest
from performanceplatform.utils.data_pusher import \
    Pusher
from mock import patch, Mock
from hamcrest import assert_that, equal_to


class TestPusher(unittest.TestCase):
    @patch('performanceplatform.utils.data_pusher.DataSet.from_config')
    def test_Pusher_init_sets_up_client_correctly(self, mock_from_config):
        data_set_config = {'beep': 'boop'}
        Pusher(data_set_config, {})
        mock_from_config.assert_called_once_with(data_set_config)

    @patch('performanceplatform.utils.data_pusher.DataSet.from_config')
    def test_pushes_with_default_chunk_100(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        data_set_config = {'beep': 'boop'}
        data = {'some': 'data'}
        Pusher(data_set_config, {}).push(data)
        mock_data_set_client.post.assert_called_once_with(data, chunk_size=100)

    @patch('performanceplatform.utils.data_pusher.DataSet.from_config')
    def test_pushes_with_chunk_in_options(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        data_set_config = {'beep': 'boop'}
        data = {'some': 'data'}
        Pusher(data_set_config, {'chunk-size': 98}).push(data)
        mock_data_set_client.post.assert_called_once_with(data, chunk_size=98)

    @patch('performanceplatform.utils.data_pusher.DataSet.from_config')
    def test_pushes_nothing_when_empty_data(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        data_set_config = {'beep': 'boop'}
        Pusher(data_set_config, {}).push([])
        assert_that(mock_data_set_client.post.called, equal_to(False))
