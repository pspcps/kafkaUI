from .postgresql import PostgreSQLDestination
from .mysql import MySQLDestination
from .sqlserver import SQLServerDestination
from .mongodb import MongoDBDestination

DESTINATIONS = {
    "postgresql": PostgreSQLDestination,
    "mysql":      MySQLDestination,
    "sqlserver":  SQLServerDestination,
    "mongodb":    MongoDBDestination,
}


def get_destination(dest_type: str, config: dict, fields: list):
    cls = DESTINATIONS.get(dest_type)
    if cls is None:
        raise ValueError(f"Unknown destination type: {dest_type}")
    return cls(config, fields)
