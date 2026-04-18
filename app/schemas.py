import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from app.database import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(..., gt=0, max_digits=20, decimal_places=2)
    currency: Currency
    description: str = Field(default="", max_length=1024)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl | None = None


class PaymentAccepted(BaseModel):
    """Response for POST /payments (202 Accepted)."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(alias="payment_metadata")
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str | None
    created_at: datetime
    processed_at: datetime | None


class WebhookPayload(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    processed_at: datetime | None


class DBStatus(BaseModel):
    status: Literal["ok", "failure"]
    error: str | None


class LivenessStatus(BaseModel):
    status: Literal["ok"]
    db: DBStatus