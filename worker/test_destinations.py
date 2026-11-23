import sys
from pathlib import Path
import unittest
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
from destinations.postgresql import PostgreSQLDestination, DestinationConfigurationError
from delivery import Processor, DeliveryBlocked, RetryPolicy
from worker.test_delivery import Message


class CredentialFailureTests(unittest.TestCase):
    def test_invalid_rotated_credentials_pause_instead_of_dead_lettering_records(self):
        destination = PostgreSQLDestination.__new__(PostgreSQLDestination)
        destination.config = {}
        destination._conn = None
        commit, dlq = Mock(), Mock()
        class ReconnectingDestination:
            def write(self, *args):
                destination._connect()
        processor = Processor(ReconnectingDestination(), commit, policy='dead_letter', dead_letter=dlq,
                              retries=RetryPolicy(3, 0, 0), sleep=lambda _: None)
        with patch('destinations.postgresql.connection_credentials', side_effect=ValueError('secret-json')):
            with self.assertRaises(DeliveryBlocked) as failure:
                processor.process(Message(), 'cluster')
        self.assertIsInstance(failure.exception.error, DestinationConfigurationError)
        self.assertEqual(failure.exception.attempts, 1)
        commit.assert_not_called()
        dlq.assert_not_called()
        self.assertNotIn('secret-json', str(failure.exception))
