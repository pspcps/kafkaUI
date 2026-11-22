"""Only adapters with a proven atomic replay guard may process new records."""
from credentials import prepare_config


def get_destination(dest_type: str, config: dict, fields: list):
    if dest_type != 'postgresql':
        raise ValueError('Replay-safe delivery currently supports PostgreSQL only; this adapter is disabled until transactional deduplication is implemented')
    from .postgresql import PostgreSQLDestination
    return PostgreSQLDestination(prepare_config(dest_type, config), fields)
