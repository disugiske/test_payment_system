"""Data-access functions for the `payments` table.

Pure SQL: no business logic, no HTTP, no broker. Each function takes an
``AsyncSession`` and returns Pydantic DTOs.
"""
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import Currency, Payment, PaymentStatus
from app.database.dto import PaymentDTO

PAYMENT_COLUMNS = (
    Payment.id,
    Payment.amount,
    Payment.currency,
    Payment.description,
    Payment.payment_metadata,
    Payment.status,
    Payment.idempotency_key,
    Payment.webhook_url,
    Payment.created_at,
    Payment.processed_at,
)


async def get_payment_by_id(
    session: AsyncSession, payment_id: uuid.UUID
) -> PaymentDTO | None:
    stmt = select(*PAYMENT_COLUMNS).where(Payment.id == payment_id)
    row = (await session.execute(stmt)).one_or_none()
    return PaymentDTO.model_validate(row) if row is not None else None


async def get_by_idempotency_key(
    session: AsyncSession, key: str
) -> PaymentDTO | None:
    stmt = select(*PAYMENT_COLUMNS).where(Payment.idempotency_key == key)
    row = (await session.execute(stmt)).one_or_none()
    return PaymentDTO.model_validate(row) if row is not None else None


async def insert_payment(
    session: AsyncSession,
    *,
    amount: Decimal,
    currency: Currency,
    description: str,
    metadata: dict[str, Any],
    idempotency_key: str,
    webhook_url: str | None,
) -> PaymentDTO:
    stmt = (
        insert(Payment)
        .values(
            amount=amount,
            currency=currency,
            description=description,
            payment_metadata=metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=webhook_url,
        )
        .returning(*PAYMENT_COLUMNS)
    )
    row = (await session.execute(stmt)).one()
    return PaymentDTO.model_validate(row)


async def update_status(
    session: AsyncSession,
    payment_id: uuid.UUID,
    status: PaymentStatus,
) -> PaymentDTO | None:
    stmt = (
        update(Payment)
        .where(Payment.id == payment_id)
        .values(status=status, processed_at=datetime.now(timezone.utc))
        .returning(*PAYMENT_COLUMNS)
    )
    row = (await session.execute(stmt)).one_or_none()
    return PaymentDTO.model_validate(row) if row is not None else None
