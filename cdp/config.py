import os
from pathlib import Path

from .postgres import PostgresStore
from .store import Store


def configured_store() -> Store:
    directory = Path(os.environ.get("CDP_DATA_DIR", "var/cdp"))
    dsn = os.environ.get("CDP_POSTGRES_DSN")
    return PostgresStore(directory, dsn) if dsn else Store(directory)
