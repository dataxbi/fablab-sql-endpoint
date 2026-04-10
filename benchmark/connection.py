"""
benchmark/connection.py
Fabric SQL endpoint connection management via pyodbc.
Connection strings are read from environment variables.

Authentication uses an Azure access token obtained from `az account get-access-token`.
This avoids browser popups and works with the az login session (ActiveDirectoryDefault
keyword is only available in newer ODBC driver builds; token injection works universally).
"""

import json
import logging
import os
import struct
import subprocess
import sys
from contextlib import contextmanager
from collections.abc import Iterator

import pyodbc
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

# ODBC driver name for Fabric SQL endpoints (ships with Microsoft ODBC Driver for SQL Server)
ODBC_DRIVER = os.getenv("ODBC_DRIVER", "ODBC Driver 18 for SQL Server")

# SQL_COPT_SS_ACCESS_TOKEN — pyodbc connection attribute for injecting a bearer token
_SQL_COPT_SS_ACCESS_TOKEN = 1256

# On Windows, az is a batch script and must be invoked as az.cmd
_AZ_CMD = "az.cmd" if sys.platform == "win32" else "az"

_CONN_STR_TEMPLATE = (
    "Driver={{{driver}}};"
    "Server={server},1433;"
    "Database={database};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)


def _get_access_token() -> bytes:
    """Obtain an Azure access token from az CLI and return it packed for pyodbc."""
    result = subprocess.run(
        [_AZ_CMD, "account", "get-access-token",
         "--resource", "https://database.windows.net/",
         "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    token: str = json.loads(result.stdout)["accessToken"]
    token_bytes = token.encode("utf-16-le")
    # pyodbc expects a bytes object: 4-byte little-endian length + raw UTF-16-LE token
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


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
    token_struct = _get_access_token()
    logger.debug("Connecting to %s / %s ...", server, database)
    conn = pyodbc.connect(
        conn_str,
        autocommit=True,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )
    logger.debug("Connection established.")
    try:
        yield conn
    finally:
        conn.close()


def get_lakehouse_connection() -> pyodbc.Connection:
    """Open a connection to the Fabric Lakehouse SQL endpoint using env vars."""
    server = os.environ["LAKEHOUSE_SERVER"]
    database = os.environ["LAKEHOUSE_DATABASE"]
    token_struct = _get_access_token()
    return pyodbc.connect(
        _build_conn_str(server, database),
        autocommit=True,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )


def get_warehouse_connection() -> pyodbc.Connection:
    """Open a connection to the Fabric Warehouse using env vars."""
    server = os.environ["WAREHOUSE_SERVER"]
    database = os.environ["WAREHOUSE_DATABASE"]
    token_struct = _get_access_token()
    return pyodbc.connect(
        _build_conn_str(server, database),
        autocommit=True,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
    )
