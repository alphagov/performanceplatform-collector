import unittest
from performanceplatform.utils.data_pusher import \
    Pusher
from mock import patch, Mock
from hamcrest import assert_that, equal_to


@patch('performanceplatform.utils.data_pusher.DataSet.from_config')
class TestPusher(unittest.TestCase):
    def setUp(self):
        self.data_set_config = {'beep': 'boop'}
        self.data = {'some': 'data'}

    def test_Pusher_init_sets_up_client_correctly(self, mock_from_config):
        Pusher(self.data_set_config, {})
        mock_from_config.assert_called_once_with(self.data_set_config)

    def test_pushes_with_default_chunk_100(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        Pusher(self.data_set_config, {}).push(self.data)
        mock_data_set_client.post.assert_called_once_with(self.data,
                                                          chunk_size=100)

    def test_pushes_with_chunk_in_options(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        Pusher(self.data_set_config, {'chunk-size': 98}).push(self.data)
        mock_data_set_client.post.assert_called_once_with(self.data,
                                                          chunk_size=98)

    def test_pushes_nothing_when_empty_data(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        Pusher(self.data_set_config, {}).push([])
        assert_that(mock_data_set_client.post.called, equal_to(False))

    def test_empties_data_set(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        Pusher(self.data_set_config, {'empty-data-set': True}).push(self.data)
        assert_that(mock_data_set_client.empty_data_set.called, equal_to(True))

    def test_does_not_empty_data_set(self, mock_from_config):
        mock_data_set_client = Mock()
        mock_from_config.return_value = mock_data_set_client
        Pusher(self.data_set_config, {}).push(self.data)
        assert_that(mock_data_set_client.empty_data_set.called,
                    equal_to(False))
