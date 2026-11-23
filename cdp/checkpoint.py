"""Keep two development Spark jobs from sharing the same checkpoint."""

from collections.abc import Iterator
from contextlib import contextmanager

import psycopg
from pydantic import TypeAdapter, ValidationError

from .contract import Key
from .kafka_recovery import recovery_scope


class CheckpointInUse(RuntimeError):
    pass


def recovery_checkpoint(base: str, cluster_id: str, run_id: str | None, target_id: str | None) -> str:
    if not base.rstrip("/"):
        raise ValueError("recovery requires a non-root checkpoint path")
    try:
        cluster = TypeAdapter(Key).validate_python(cluster_id)
    except ValidationError:
        raise ValueError("Kafka cluster ID cannot be used as a checkpoint name") from None
    _, scope = recovery_scope(run_id, target_id)
    return f"{base.rstrip('/')}.recovery/{cluster}.{scope}"


@contextmanager
def checkpoint_lease(dsn: str, checkpoint: str) -> Iterator[psycopg.Connection]:
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as connection:
        acquired = connection.execute(
            "SELECT pg_try_advisory_lock(387216, hashtext(%s))", (checkpoint,)
        ).fetchone()[0]
        if not acquired:
            raise CheckpointInUse("another Spark job is using this checkpoint")
        # Closing the session releases the lease, including when the driver exits abruptly.
        yield connection
