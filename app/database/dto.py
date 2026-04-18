"""Pydantic DTOs returned by the data-access layer.

These are internal representations of DB rows: they are decoupled from the
ORM session (no lazy-loading surprises) and from the API contract
(``app.schemas``).
"""
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.database import Currency, PaymentStatus


class PaymentDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    payment_metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str | None
    created_at: datetime
    processed_at: datetime | None


class OutboxEventDTO(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    aggregate_id: uuid.UUID
    event_type: str
    payload: dict[str, Any]
    routing_key: str
    attempts: int
