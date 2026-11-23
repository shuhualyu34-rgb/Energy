from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

Key = Annotated[str, Field(strict=True, pattern=r"^[A-Za-z0-9_-]{1,64}$")]
PositiveInteger = Annotated[int, Field(strict=True, gt=0, le=2**53 - 1)]
Quantity = Annotated[int, Field(strict=True, ge=0, le=2**53 - 1)]


class Order(BaseModel):
    """Versioned, synthetic completed-order snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["0.1"]
    order_id: Key
    station_id: Key
    version: PositiveInteger
    started_at: AwareDatetime
    ended_at: AwareDatetime
    energy_wh: Quantity
    electricity_fee_cents: Quantity
    service_fee_cents: Quantity

    @field_validator("started_at", "ended_at", mode="before")
    @classmethod
    def require_timestamp_string(cls, value: object) -> object:
        if not isinstance(value, str) or "T" not in value or len(value) < 20:
            raise ValueError("expected an ISO 8601 timestamp with an explicit offset")
        return value

    @model_validator(mode="after")
    def check_times(self) -> "Order":
        if self.ended_at <= self.started_at:
            raise ValueError("ended_at must follow started_at")
        if self.ended_at > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("ended_at exceeds allowed clock skew")
        return self
