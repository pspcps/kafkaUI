import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from worker import credentials


class ConnectionCredentialTests(unittest.TestCase):
    def profile(self, provider='kubernetes_secret'):
        return dict(id='approved', provider=provider, destinationType='postgresql', caSecretName='rds-ca',
                    host='db.example', port=5432, database='analytics', username='worker', region='us-east-1')

    def test_no_inline_or_mismatched_credentials(self):
        with patch.dict(os.environ, {'DESTINATION_PROFILE_JSON': json.dumps(self.profile())}):
            for cfg in ({'password': 'secret'}, {'connectionId': 'other'}, {'connectionId': 'approved', 'tableName': 'orders;DROP'}):
                with self.assertRaises(ValueError): credentials.prepare_config('postgresql', cfg)

    def test_mount_rotation_and_secret_error_redaction(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'credentials.json'
            with patch.object(credentials, 'CREDENTIAL_FILE', path), patch.dict(os.environ, {'DESTINATION_PROFILE_JSON': json.dumps(self.profile())}):
                config=credentials.prepare_config('postgresql', {'connectionId':'approved', 'tableName':'orders'})
                payload=dict(host='db.example',database='analytics',username='worker',password='first-secret')
                path.write_text(json.dumps(payload))
                first=credentials.connection_credentials(config)
                self.assertEqual(first['password'], 'first-secret')
                self.assertEqual(first['sslmode'], 'verify-full')
                self.assertEqual(first['sslRootCert'], credentials.CA_FILE)
                payload['password']='rotated-secret';path.write_text(json.dumps(payload))
                self.assertEqual(credentials.connection_credentials(config)['password'], 'rotated-secret')
                self.assertNotIn('password', config)
                path.write_text('{secret material must not leak')
                with self.assertRaisesRegex(ValueError, '^Mounted connection credentials are missing or invalid$'):
                    credentials.connection_credentials(config)

    def test_iam_token_generated_for_every_connection(self):
        sdk=Mock();sdk.client.return_value.generate_db_auth_token.side_effect=['token1','token2']
        with patch.dict('sys.modules', {'boto3':sdk}), patch.dict(os.environ, {'DESTINATION_PROFILE_JSON':json.dumps(self.profile('rds_iam'))}):
            config=credentials.prepare_config('postgresql', {'connectionId':'approved','tableName':'orders'})
            self.assertEqual(credentials.connection_credentials(config)['password'], 'token1')
            self.assertEqual(credentials.connection_credentials(config)['password'], 'token2')
            sdk.client.return_value.generate_db_auth_token.assert_called_with(DBHostname='db.example',Port=5432,DBUsername='worker',Region='us-east-1')
            self.assertNotIn('password',config)

    def test_unknown_provider_and_missing_tls_fail_closed(self):
        for provider,ca in [('unknown','ca'),('rds_iam','')]:
            profile=self.profile(provider);profile['caSecretName']=ca
            with self.assertRaises(ValueError):
                # Use mounted case for TLS check to avoid calling any AWS service.
                if provider=='unknown': credentials.connection_credentials({'_profile':profile})
                else:
                    sdk=Mock()
                    with patch.dict('sys.modules',{'boto3':sdk}): credentials.connection_credentials({'_profile':profile})
