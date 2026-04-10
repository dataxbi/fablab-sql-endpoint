"""
benchmark/connection.py
Fabric SQL endpoint connection management via pyodbc.
Connection strings are read from environment variables.
"""

import logging
import os
from contextlib import contextmanager
from collections.abc import Iterator

import pyodbc
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ODBC driver name for Fabric SQL endpoints (ships with Microsoft ODBC Driver for SQL Server)
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# Connection string template — ActiveDirectoryDefault uses az login token (no browser popup)
_CONN_STR_TEMPLATE = (
    "Driver={{{driver}}};"
    "Server={server},1433;"
    "Database={database};"
    "Authentication=ActiveDirectoryDefault;"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


def _build_conn_str(server: str, database: str) -> str:
    return _CONN_STR_TEMPLATE.format(
        driver=ODBC_DRIVER,
        server=server,
        database=database,
    )


@contextmanager
def get_connection(server: str, database: str) -> Iterator[pyodbc.Connection]:
    """Context manager: open a pyodbc connection, yield it, then close it."""
    conn_str = _build_conn_str(server, database)
    logger.debug("Connecting to %s / %s ...", server, database)
    conn = pyodbc.connect(conn_str, autocommit=True)
    logger.debug("Connection established.")
    try:
        yield conn
    finally:
        conn.close()


def get_lakehouse_connection() -> pyodbc.Connection:
    """Open a connection to the Fabric Lakehouse SQL endpoint using env vars."""
    server = os.environ["LAKEHOUSE_SERVER"]
    database = os.environ["LAKEHOUSE_DATABASE"]
    return pyodbc.connect(_build_conn_str(server, database), autocommit=True)


def get_warehouse_connection() -> pyodbc.Connection:
    """Open a connection to the Fabric Warehouse using env vars."""
    server = os.environ["WAREHOUSE_SERVER"]
    database = os.environ["WAREHOUSE_DATABASE"]
    return pyodbc.connect(_build_conn_str(server, database), autocommit=True)
