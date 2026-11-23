"""Provision the narrowly scoped PostgreSQL role used by the development API."""

import os

from psycopg import sql

from .config import configured_store
from .postgres import READ_RELATIONS, PostgresStore

READER = "cdp_api_reader"
READER_COMMENT = "CDP-1 development API reader"


def provision(store: PostgresStore, password: str) -> None:
    store.initialize()
    role = sql.Identifier(READER)
    with store.connect() as db:
        db.execute("SELECT pg_advisory_xact_lock(387217, 1)")
        previous = db.execute("""
            SELECT shobj_description(oid, 'pg_authid') AS note FROM pg_roles WHERE rolname = %s
        """, (READER,)).fetchone()
        if previous and previous["note"] != READER_COMMENT:
            raise ValueError("existing database role does not belong to the CDP development initializer")
        if not previous:
            db.execute(sql.SQL("""
                CREATE ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
                    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD {}
            """).format(role, sql.Literal(password)))
            db.execute(sql.SQL("COMMENT ON ROLE {} IS {}").format(role, sql.Literal(READER_COMMENT)))
        database = sql.Identifier(db.info.dbname)
        db.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, role))
        db.execute(sql.SQL("GRANT USAGE ON SCHEMA public TO {}").format(role))
        relations = sql.SQL(", ").join(sql.Identifier("public", name) for name in READ_RELATIONS)
        db.execute(sql.SQL("GRANT SELECT ON {} TO {}").format(relations, role))


def main() -> None:
    store = configured_store()
    password = os.getenv("CDP_DEV_READER_PASSWORD")
    if not isinstance(store, PostgresStore) or not password:
        raise SystemExit("PostgreSQL owner connection and development reader password are required")
    try:
        provision(store, password)
    except ValueError as error:
        raise SystemExit(str(error)) from None
    print("Development PostgreSQL reader is ready.")


if __name__ == "__main__":
    main()
