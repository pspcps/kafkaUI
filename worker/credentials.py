"""Resolve approved connection references inside the worker, never in the UI/API.
Vault and AWS Secrets Manager use the operator-installed Secrets Store CSI driver.
"""
import json
import os
from pathlib import Path
import re

CREDENTIAL_FILE = Path('/var/run/kafkaui-credentials/credentials.json')
CA_FILE = '/var/run/kafkaui-ca/ca.pem'
IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]{0,62}$')


def prepare_config(destination_type: str, config: dict) -> dict:
    if any(config.get(k) for k in ('password', 'host', 'username', 'database', 'port')):
        raise ValueError('Legacy inline destination credentials are disabled; migrate to an approved connection')
    try:
        profile = json.loads(os.environ['DESTINATION_PROFILE_JSON'])
    except (KeyError, ValueError):
        raise ValueError('A worker connection profile is required') from None
    if not config.get('connectionId') or config['connectionId'] != profile.get('id') or destination_type != profile.get('destinationType'):
        raise ValueError('Worker connection profile does not match the job')
    result = {'_profile': profile}
    for name in ('tableName', 'collection', 'schema'):
        if config.get(name):
            if not IDENTIFIER.fullmatch(config[name]):
                raise ValueError('Invalid destination identifier')
            result[name] = config[name]
    return result


def connection_credentials(config: dict) -> dict:
    """Called at connection creation; do not cache IAM tokens for future reconnects."""
    profile = config['_profile']
    provider = profile.get('provider')
    if profile['destinationType'] in ('postgresql', 'mysql') and not profile.get('caSecretName'):
        raise ValueError('Verified TLS CA configuration is required')
    if provider == 'rds_iam':
        if profile.get('destinationType') not in ('postgresql', 'mysql'):
            raise ValueError('RDS IAM requires PostgreSQL or MySQL')
        import boto3
        values = {k: profile[k] for k in ('host', 'port', 'database', 'username')}
        # IRSA, EKS Pod Identity or another default-chain workload identity.
        values['password'] = boto3.client('rds', region_name=profile['region']).generate_db_auth_token(
            DBHostname=values['host'], Port=values['port'], DBUsername=values['username'], Region=profile['region'],
        )
    elif provider in ('vault', 'aws_secrets_manager', 'kubernetes_secret'):
        try:
            with CREDENTIAL_FILE.open('r', encoding='utf-8') as handle:
                raw = handle.read(1024 * 1024 + 1)
            if len(raw) > 1024 * 1024:
                raise ValueError('Credential document too large')
            payload = json.loads(raw)
            values = {k: payload[k] for k in ('host', 'database', 'username', 'password')}
            defaults = {'postgresql': 5432, 'mysql': 3306, 'mongodb': 27017, 'sqlserver': 1433}
            values['port'] = int(payload.get('port', defaults[profile['destinationType']]))
            if not all(isinstance(values[k], str) and values[k] for k in ('host', 'database', 'username', 'password')) or not 0 < values['port'] < 65536:
                raise ValueError('Invalid credentials')
        except (OSError, ValueError, KeyError, TypeError):
            # Never include secret JSON or parser diagnostics in callback/log output.
            raise ValueError('Mounted connection credentials are missing or invalid') from None
    else:
        raise ValueError('Unsupported worker credential provider')
    values.update({k: v for k, v in config.items() if k != '_profile'})
    values['sslmode'] = 'verify-full'
    if profile.get('caSecretName'):
        values['sslRootCert'] = CA_FILE
    return values
