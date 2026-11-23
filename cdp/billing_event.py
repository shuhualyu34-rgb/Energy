import hashlib
import json

TOPIC = "billing.orders.v1"


def event_id(order_id: str, version: int, updated_at: str) -> str:
    position = json.dumps((order_id, version, updated_at), separators=(",", ":"))
    return hashlib.sha256(position.encode()).hexdigest()

BILLING_ARCHIVE = "billing-v1"


def billing_payload(
    position: tuple[str, int, str], status: str, snapshot: str, slices: str | None,
) -> bytes:
    order_id, version, updated = position
    return json.dumps({
        "schema_version": "1.0",
        "event_id": event_id(*position),
        "order_id": order_id,
        "version": version,
        "updated_at": updated,
        "status": status,
        "order": json.loads(snapshot),
        "tou_slices": json.loads(slices) if slices is not None else None,
    }, separators=(",", ":")).encode()
