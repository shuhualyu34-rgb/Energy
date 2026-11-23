import os
from pathlib import Path

from .store import Store


def configured_store() -> Store:
    directory = Path(os.environ.get("CDP_DATA_DIR", "var/cdp"))
    dsn = os.environ.get("CDP_POSTGRES_DSN")
    if not dsn:
        return Store(directory)

    from .postgres import PostgresStore

    return PostgresStore(directory, dsn)
